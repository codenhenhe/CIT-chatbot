from fastapi import APIRouter
import re
import os
import asyncio
import time
from app.scripts.prototype import GraphRAGChatbot
from pydantic import BaseModel
from typing import List, Optional
import ollama
import json
from fastapi.responses import StreamingResponse

router = APIRouter()

bot = GraphRAGChatbot()

class MessagePart(BaseModel):
    type: str
    text: str

class MessageItem(BaseModel):
    id: Optional[str] = None
    role: str
    parts: List[MessagePart]

class ChatRequest(BaseModel):
    id: Optional[str] = None
    messages: List[MessageItem]
    trigger: Optional[str] = None

ROUTER_MODEL = os.getenv("ROUTER_MODEL", "qwen2.5-coder:1.5b-instruct")
FINAL_PRIMARY_MODEL = os.getenv("FINAL_PRIMARY_MODEL", "qwen2.5-coder:7b-instruct")
FINAL_FALLBACK_MODEL = os.getenv("FINAL_FALLBACK_MODEL", "qwen2.5-coder:3b-instruct")
FIRST_TOKEN_TIMEOUT_SECONDS = float(os.getenv("FIRST_TOKEN_TIMEOUT_SECONDS", "12"))


async def _stream_chat_with_first_token_timeout(client, model: str, messages: list, first_token_timeout: float):
    """
    Stream từ Ollama và timeout nếu token đầu tiên trả về quá chậm.
    Mục tiêu là phát hiện model bị nghẽn tải để fallback sớm.
    """
    stream = await client.chat(
        model=model,
        messages=messages,
        stream=True,
    )

    first_token = True
    while True:
        try:
            if first_token:
                part = await asyncio.wait_for(anext(stream), timeout=first_token_timeout)
                first_token = False
            else:
                part = await anext(stream)
        except StopAsyncIteration:
            break
        yield part

def detect_intent(query: str):
    q = query.lower().strip()

    if len(q) < 3:
        return "SMALL_TALK"

    # ===== BASIC =====
    if re.fullmatch(r"(xin chào|chào|hi|hello|hey)+", q):
        return "GREETING"

    if any(x in q for x in ["cảm ơn", "thanks"]):
        return "THANKS"

    if any(x in q for x in ["tạm biệt", "bye"]):
        return "GOODBYE"

    # ===== FEEDBACK =====
    if any(x in q for x in ["sai", "sai rồi", "không đúng", "nhầm"]):
        return "CORRECTION"

    if any(x in q for x in ["ý tôi là", "tôi muốn hỏi", "ý mình là"]):
        return "REFINE"

    # ===== CTU DOMAIN =====
    if any(x in q for x in ["bao nhiêu tín chỉ", "tín chỉ"]):
        return "CREDIT"

    if any(x in q for x in ["môn học", "học phần"]):
        return "COURSE"

    if any(x in q for x in ["chương trình đào tạo", "ctdt"]):
        return "CURRICULUM"

    if any(x in q for x in ["ngành", "chuyên ngành"]):
        return "MAJOR"

    if any(x in q for x in ["học phí"]):
        return "TUITION"

    if any(x in q for x in ["điều kiện", "yêu cầu", "đầu vào"]):
        return "REQUIREMENT"

    if any(x in q for x in ["ở đâu", "khoa nào", "thuộc khoa"]):
        return "FACULTY"

    # ===== fallback =====
    return "RAG"


def build_greeting_response(user_query: str, history: List[dict]) -> str:
    """Sinh phản hồi greeting linh hoạt bằng rule-based để giữ latency thấp."""
    q = user_query.lower().strip()
    prior_turns = len(history)

    asks_major = any(x in q for x in ["ngành", "chuyên ngành", "ctdt", "chương trình"])
    asks_course = any(x in q for x in ["môn", "học phần", "tín chỉ", "tiên quyết"])
    asks_rule = any(x in q for x in ["quy chế", "điều kiện", "gpa", "học vụ"])

    if prior_turns > 0:
        prefix_pool = ["Chào bạn quay lại!", "Rất vui được gặp lại bạn!", "Chào mừng bạn trở lại!"]
        prefix = prefix_pool[int(time.time()) % len(prefix_pool)]
    elif any(x in q for x in ["buổi sáng", "sáng"]):
        prefix = "Chào buổi sáng!"
    elif any(x in q for x in ["buổi chiều", "chiều"]):
        prefix = "Chào buổi chiều!"
    elif any(x in q for x in ["buổi tối", "tối"]):
        prefix = "Chào buổi tối!"
    else:
        prefix_pool = ["Xin chào!", "Chào bạn!", "Chào bạn nhe!", "Rất vui được hỗ trợ bạn!"]
        prefix = prefix_pool[int(time.time()) % len(prefix_pool)]

    if asks_major:
        hint = "Mình có thể hỗ trợ thông tin ngành, khung CTĐT, thời gian học và văn bằng. Bạn muốn xem tổng quan hay chi tiết từng ngành?"
    elif asks_course:
        hint = "Mình có thể tra tín chỉ, môn tiên quyết, và thông tin học phần cụ thể. Bạn có mã môn hoặc tên môn không?"
    elif asks_rule:
        hint = "Mình có thể tra quy chế học vụ, điều kiện học và các mốc học tập quan trọng. Bạn muốn tra mục nào trước?"
    else:
        hint_pool = [
            "Bạn muốn tra cứu ngành, học phần hay quy chế học vụ trước?",
            "Mình có thể hỗ trợ ngành đào tạo, học phần và quy chế học vụ. Bạn muốn bắt đầu từ phần nào?",
            "Bạn cần mình tra nhanh số tín chỉ, danh sách ngành hay môn tiên quyết?",
        ]
        hint = hint_pool[(int(time.time()) + prior_turns) % len(hint_pool)]

    return f"{prefix} {hint}"

async def rewrite_query(client, history, query):
    # chặn câu quá ngắn
    if len(query.strip()) < 5:
        return query

    prompt = (
        "Nhiệm vụ: Viết lại câu hỏi người dùng cho rõ nghĩa hơn.\n"
        "- KHÔNG được trả lời.\n"
        "- KHÔNG thêm thông tin mới.\n"
        "- KHÔNG suy đoán.\n"
        "- Chỉ output duy nhất câu hỏi đã rewrite.\n"
        "- Nếu câu đã rõ → giữ nguyên.\n"
    )

    messages = history[-3:] + [{"role": "user", "content": query}]

    resp = await client.chat(
        model=ROUTER_MODEL,
        messages=[{"role": "system", "content": prompt}] + messages,
        options=ollama_options,
    )

    rewritten = resp['message']['content'].strip()

    # Guard lần 2: nếu nó trả lời → bỏ
    if any(x in rewritten.lower() for x in ["tín chỉ", "là", "bao gồm"]):
        return query

    return rewritten

@router.post("/chat")
async def chat_endpoint(body: ChatRequest):
    last_message = body.messages[-1]
    user_query = last_message.parts[0].text

    print(f"\n=== USER: {user_query} ===")

    history = [
        {"role": m.role, "content": m.parts[0].text}
        for m in body.messages[:-1]
    ]
    print("History:", history)

    # Rule-based greeting nhanh để giảm tải model
    if detect_intent(user_query) == "GREETING":
        greeting_text = build_greeting_response(user_query, history)

        async def stream_results():
            yield f'0:{json.dumps(greeting_text)}\n'

        return StreamingResponse(
            stream_results(),
            media_type="text/plain",
            headers={"X-Vercel-AI-Data-Stream": "v1"}
        )

    # Main pipeline: rewrite -> intent -> cypher -> graph/vector -> context
    try:
        pipeline_out = await bot.run_main_pipeline(user_query, history=history[-5:])
    except Exception as e:
        print("ERROR run_main_pipeline:", e)
        pipeline_out = {
            "rewritten_query": user_query,
            "intent_type": "factual_graph",
            "context": "",
            "cypher": None,
        }

    rewritten_query = pipeline_out.get("rewritten_query", user_query)
    intent_type = pipeline_out.get("intent_type", "factual_graph")
    context = pipeline_out.get("context", "")
    cypher = pipeline_out.get("cypher")
    graph_rows = pipeline_out.get("graph_rows", [])
    stage_metrics = pipeline_out.get("metrics", {})
    metrics_summary = pipeline_out.get("metrics_summary", {})

    print(f"IntentType: {intent_type}")
    print(f"Rewrite: {rewritten_query}")
    print(f"Cypher: {cypher}")
    print(f"GraphRowsCount: {len(graph_rows)}")
    if graph_rows:
        print(f"GraphRowsPreview: {graph_rows[:2]}")
    print(
        "StageMetrics:",
        {
            "rewrite_ms": stage_metrics.get("rewrite_ms"),
            "graph_ms": stage_metrics.get("graph_ms"),
            "cypher_generated": stage_metrics.get("cypher_generated"),
            "cypher_source": stage_metrics.get("cypher_source"),
            "graph_hit": stage_metrics.get("graph_hit"),
            "graph_rows_count": stage_metrics.get("graph_rows_count"),
            "fallback": stage_metrics.get("fallback"),
        },
    )
    print(
        "Rates:",
        {
            "cypher_hit_rate": metrics_summary.get("cypher_hit_rate"),
            "template_hit_rate": metrics_summary.get("template_hit_rate"),
            "llm_cypher_hit_rate": metrics_summary.get("llm_cypher_hit_rate"),
            "template_share_in_cypher": metrics_summary.get("template_share_in_cypher"),
            "llm_share_in_cypher": metrics_summary.get("llm_share_in_cypher"),
            "graph_hit_rate": metrics_summary.get("graph_hit_rate"),
            "fallback_rate": metrics_summary.get("fallback_rate"),
            "avg_rewrite_ms": metrics_summary.get("avg_rewrite_ms"),
            "avg_graph_ms": metrics_summary.get("avg_graph_ms"),
        },
    )

    print(f"=== CONTEXT: {context} ===")

    if intent_type in ["factual_graph", "explanation"] and not context:
        no_data_msg = bot.build_no_data_reply(rewritten_query, context="")
        async def stream_results():
            yield f'0:{json.dumps(no_data_msg)}\n'

        return StreamingResponse(
            stream_results(),
            media_type="text/plain",
            headers={"X-Vercel-AI-Data-Stream": "v1"}
        )

    # BƯỚC 2: ANSWER GENERATION (strict grounded) + stream chunk
    async def stream_results():
        if intent_type == "chitchat":
            q_norm = bot._normalize_text(user_query)
            if any(x in q_norm for x in ["tra loi bang tieng viet", "trả lời bằng tiếng việt", "noi tieng viet", "nói tiếng việt"]):
                yield f'0:{json.dumps("Được bạn, từ bây giờ mình sẽ trả lời bằng tiếng Việt. Bạn muốn tra cứu nội dung nào của CTU?")}\n'
                return

            client = ollama.AsyncClient()
            prompt = (
                "Bạn là trợ lý thân thiện của CTU. "
                "Luôn trả lời bằng tiếng Việt, ngắn gọn, lịch sự, không dùng tiếng Anh nếu không được yêu cầu."
            )
            try:
                resp = await client.chat(
                    model=ROUTER_MODEL,
                    messages=[
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": rewritten_query},
                    ],
                )
                answer = (resp.get("message", {}).get("content") or "Xin chào, mình có thể hỗ trợ gì thêm?").strip()
            except Exception:
                answer = "Xin chào, mình có thể hỗ trợ gì thêm?"
            yield f'0:{json.dumps(answer)}\n'
            return

        answer = await bot.answer_generator(rewritten_query, context, history=history[-5:])
        if not answer:
            answer = "Không tìm thấy thông tin"
        yield f'0:{json.dumps(answer)}\n'

    return StreamingResponse(
        stream_results(),
        media_type="text/plain",
        headers={"X-Vercel-AI-Data-Stream": "v1"}
    )
