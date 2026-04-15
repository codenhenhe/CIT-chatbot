import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from app.services.classifier_service import IntentClassifierService
from app.services.domain_service import DomainService
from app.services.entity_service import EntityService
from app.services.evaluator_service import EvaluatorService
from app.services.llm_service import LLMService
from app.services.quyche_service import quyche_service
from app.services.retrieval_service import RetrievalResult, RetrievalService
from app.services.strategy_service import StrategyService

logger = logging.getLogger("app.chat")


@dataclass
class ChatPipelineState:
    query: str
    needs_query: bool = True
    query_gate_reason: str = ""
    domain: str = ""
    intent: str = ""
    entities: Dict[str, str] = field(default_factory=dict)
    strategy: str = ""
    top_k: int = 4
    rewritten_query: str = ""
    context: str = ""
    retries: int = 0
    max_retries: int = 2


def _truncate_text(text: str, limit: int = 240) -> str:
    value = (text or "").strip()
    return value if len(value) <= limit else value[:limit] + "...<truncated>"

def _sanitize_history(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    cleaned = []
    for msg in (history or [])[-8:]:
        role = str(msg.get("role", "user")).strip().lower()
        content = str(msg.get("content", "")).strip()
        if role not in ["user", "assistant"]:
            role = "user"
        if content:
            cleaned.append({"role": role, "content": content})
    return cleaned


class ChatService:
    """Main orchestration service for chatbot request pipeline."""

    def __init__(self) -> None:
        self.llm_service = LLMService()
        self.domain_service = DomainService(self.llm_service)
        self.entity_service = EntityService(self.llm_service)
        self.intent_service = IntentClassifierService(self.llm_service)
        self.strategy_service = StrategyService()
        self.retrieval_service = RetrievalService()
        self.evaluator_service = EvaluatorService()


    async def handle_query(self, query: str, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        clean_history = _sanitize_history(history or [])
        state = ChatPipelineState(query=(query or "").strip())
        logger.info("[chat][step] request_received query=%s", _truncate_text(state.query))

        if not state.query:
            return {
                "answer": "Bạn vui lòng nhập câu hỏi cụ thể để mình hỗ trợ.",
                "state": state.__dict__,
            }

        logger.info("[chat][step] query_gate")
        history_text = "\n".join(f"{m['role']}: {m['content']}" for m in clean_history[-6:])
        gate = await self.llm_service.should_query(state.query, history_text)
        state.needs_query = bool(gate.get("need_query", True))
        state.query_gate_reason = str(gate.get("reason", ""))
        logger.info("[chat][step] query_gate need_query=%s reason=%s", state.needs_query, state.query_gate_reason)

        if not state.needs_query:
            logger.info("[chat][step] skip_retrieval_pipeline")
            answer = await self.llm_service.generate_without_query(state.query, history_text)
            return {
                "answer": answer or "Mình luôn sẵn sàng hỗ trợ bạn khi cần tra cứu thông tin đào tạo.",
                "state": state.__dict__,
            }

        logger.info("[chat][step] classify_domain")
        state.domain = await self.domain_service.classify(state.query)
        logger.info("[chat][step] domain=%s", state.domain)

        if state.domain == "quy_che":
            logger.info("[chat][step] route_to_quy_che")
            result = await quyche_service.handle_query(state.query, clean_history)
            return {"answer": result["answer"], "state": state.__dict__}

        logger.info("[chat][step] route_to_ctdt_pipeline")
        answer = await self._run_ctdt_pipeline(state, clean_history)
        return {"answer": answer, "state": state.__dict__}

    async def _run_ctdt_pipeline(self, state: ChatPipelineState, history: List[Dict[str, str]]) -> str:
        _ = history
        logger.info("[chat][step] rewrite_query")
        state.rewritten_query = await self.llm_service.rewrite(state.query)
        logger.info("[chat][step] rewritten=%s", _truncate_text(state.rewritten_query))

        logger.info("[chat][step] extract_entities")
        state.entities = await self.entity_service.extract(state.rewritten_query)
        logger.info("[chat][step] entities=%s", state.entities)

        logger.info("[chat][step] classify_intent")
        state.intent = await self.intent_service.classify(
            query=state.rewritten_query,
            entities=state.entities,
            domain=state.domain,
        )
        logger.info("[chat][step] intent=%s", state.intent)

        logger.info("[chat][step] select_strategy")
        state.strategy = await self.strategy_service.select(state.domain, state.intent)
        logger.info("[chat][step] strategy=%s top_k=%s", state.strategy, state.top_k)

        last_retrieval: RetrievalResult | None = None
        while state.retries <= state.max_retries:
            logger.info("[chat][step] retrieval attempt=%s", state.retries + 1)
            last_retrieval = await self.retrieval_service.execute(
                strategy=state.strategy,
                query=state.rewritten_query,
                entities=state.entities,
                top_k=state.top_k,
            )
            state.context = last_retrieval.context
            logger.info("[chat][step] retrieval_done strategy=%s context_len=%s", state.strategy, len(state.context or ""))

            logger.info("[chat][step] evaluate_context")
            evaluation = await self.evaluator_service.check(state.context)
            logger.info("[chat][step] evaluation is_enough=%s action=%s reason=%s", evaluation.is_enough, evaluation.action, evaluation.reason)
            if evaluation.is_enough:
                break

            if state.retries == state.max_retries:
                break

            if await self.evaluator_service.need_more_context(evaluation):
                state.top_k += 2
                logger.info("[chat][retry] increase_top_k=%s", state.top_k)
            elif await self.evaluator_service.need_rewrite(evaluation):
                logger.info("[chat][retry] rewrite_pipeline")
                state.rewritten_query = await self.llm_service.rewrite(state.rewritten_query, feedback=evaluation.reason)
                state.entities = await self.entity_service.extract(state.rewritten_query)
                state.intent = await self.intent_service.classify(
                    query=state.rewritten_query,
                    entities=state.entities,
                    domain=state.domain,
                )
                state.strategy = await self.strategy_service.select(state.domain, state.intent)
            elif await self.evaluator_service.need_switch(evaluation):
                state.strategy = await self.strategy_service.switch(state.strategy)
                logger.info("[chat][retry] switch_strategy=%s", state.strategy)
            else:
                state.top_k += 1
                logger.info("[chat][retry] conservative_top_k=%s", state.top_k)

            state.retries += 1

        final_context = state.context
        if not final_context and last_retrieval:
            final_context = last_retrieval.context

        logger.info("[chat][step] generate_answer")
        answer = await self.llm_service.generate(state.rewritten_query or state.query, final_context)
        logger.info("[chat][step] generation_done answer_len=%s", len(answer or ""))
        if not answer:
            return "Mình chưa tìm được đủ dữ liệu để trả lời chính xác. Bạn có thể mô tả chi tiết hơn không?"
        return answer


chat_service = ChatService()


async def chat_handler(question: str, history: List[Dict[str, Any]]) -> str:
    try:
        result = await chat_service.handle_query(question, history)
        return str(result.get("answer", "Không tìm thấy thông tin."))
    except Exception:
        logger.exception("[chat] unhandled pipeline error")
        return "Hệ thống đang bận hoặc truy vấn quá lâu. Bạn vui lòng thử lại với câu hỏi ngắn gọn hơn."