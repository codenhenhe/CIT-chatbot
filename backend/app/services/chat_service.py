import json
import logging
import re
from typing import Any, Dict, List

from app.services.llm_service import call_model_3b, call_model_3b_json, call_model_7b
from app.services.retrieval_service import retrieve_graph_context


logger = logging.getLogger("app.chat")


def _truncate_text(text: str, limit: int = 1200) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "...<truncated>"


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ["true", "1", "yes"]
    return False


def _sanitize_history(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    cleaned = []
    for msg in history[-8:]:
        role = str(msg.get("role", "user")).strip().lower()
        content = str(msg.get("content", "")).strip()
        if role not in ["user", "assistant"]:
            role = "user"
        if not content:
            continue
        cleaned.append({"role": role, "content": content})
    return cleaned


async def rewrite_question(question: str, history: List[Dict[str, str]]) -> str:
    raw = (question or "").strip()
    if len(raw) < 3:
        return raw

    norm = raw.lower().strip()
    if norm in ["co", "có"]:
        return "Người dùng xác nhận yêu cầu trước đó"

    prompt = (
        "STRICT CONTROL PROMPT FOR GRAPHRAG PIPELINE\n"
        "Bạn là thành phần QUERY REWRITE duy nhất.\n"
        "GLOBAL RULES:\n"
        "- KHÔNG trả lời user\n"
        "- KHÔNG giải thích\n"
        "- KHÔNG thêm dữ kiện mới\n"
        "- Nếu câu đã rõ thì giữ nguyên\n"
        "- Nếu mơ hồ thì mở rộng từ lịch sử\n"
        "- Nếu không chắc: trả lại câu gốc\n"
        "Output duy nhất là câu rewrite."
    )

    history_text = "\n".join([f"{m['role']}: {m['content']}" for m in history[-5:]])
    llm_input = (
        f"Lịch sử:\n{history_text}\n\n"
        f"Query gốc: {raw}\n\n"
        "Thực hiện TASK 1: QUERY REWRITE."
    )

    try:
        rewritten = await call_model_3b(f"{prompt}\n\n{llm_input}")
    except Exception:
        logger.exception("[chat][rewrite] Rewrite model call failed")
        return raw
    rewritten = (rewritten or "").strip()
    if not rewritten:
        return raw

    rw_lower = rewritten.lower().strip()
    if rw_lower.startswith("trả lời") or rw_lower.startswith("câu trả lời"):
        return raw

    if len(rewritten) > 512:
        return raw

    return rewritten


# ===== STEP 1: classify =====
async def classify_question(question: str) -> Dict[str, Any]:
    """
    Decide:
    - có cần RAG không
    - dùng model nào
    """

    prompt = f"""
Phân loại câu hỏi bên dưới.
Chỉ trả về JSON hợp lệ, không markdown, không giải thích.

Câu hỏi: "{question}"

Schema bắt buộc:
{{
  "use_rag": true/false,
  "complexity": "simple" | "complex"
}}
"""

    try:
        data = await call_model_3b_json(prompt)
    except Exception:
        logger.exception("[chat][classify] Classify model call failed")
        data = {}

    if not data:
        q = (question or "").lower()
        if any(k in q for k in ["bao nhiêu", "tín chỉ", "chương trình", "ngành", "quy chế", "điều kiện", "clc"]):
            return {"use_rag": True, "complexity": "complex"}
        return {"use_rag": False, "complexity": "simple"}

    use_rag = _normalize_bool(data.get("use_rag"))
    complexity = str(data.get("complexity", "simple")).strip().lower()
    if complexity not in ["simple", "complex"]:
        complexity = "complex" if use_rag else "simple"

    return {"use_rag": use_rag, "complexity": complexity}


# ===== STEP 2: build prompt =====
def build_prompt(question: str, context: str, history: List[Dict[str, str]]) -> str:
    history_text = "\n".join([f"{m['role']}: {m['content']}" for m in history])

    return f"""
Bạn là chatbot tư vấn về chương trình đào tạo và quy chế học vụ của Đại học Cần Thơ (CTU).

Nguyên tắc bắt buộc:
- Chỉ dùng Context (bao gồm GRAPH/VECTOR nếu có) để trả lời phần dữ liệu chuyên môn.
- Nếu Context không đủ, trả về đúng: Không tìm thấy thông tin.
- Không bịa, không suy đoán.

Lịch sử:
{history_text}

Context:
{context}

Câu hỏi:
{question}

Trả lời rõ ràng, đúng trọng tâm.
"""


# ===== MAIN PIPELINE =====
async def chat_handler(question: str, history: List[Dict[str, Any]]):
    clean_history = _sanitize_history(history or [])
    logger.info(
        "[chat][request] question=%s history_count=%d history=%s",
        _truncate_text(question, 500),
        len(clean_history),
        _truncate_text(json.dumps(clean_history, ensure_ascii=False), 1800),
    )

    # 0. rewrite
    rewritten_question = await rewrite_question(question, clean_history)
    logger.info(
        "[chat][rewrite] original=%s rewritten=%s changed=%s",
        _truncate_text(question, 500),
        _truncate_text(rewritten_question, 500),
        str((question or "").strip() != rewritten_question),
    )

    # 1. classify
    meta = await classify_question(rewritten_question)
    use_rag = bool(meta.get("use_rag"))
    complexity = str(meta.get("complexity", "simple"))
    logger.info(
        "[chat][classify] rewritten=%s meta=%s",
        _truncate_text(rewritten_question, 500),
        json.dumps(meta, ensure_ascii=False),
    )

    # Guard: câu hỏi dữ kiện thường nên bật RAG.
    q_norm = rewritten_question.lower()
    if any(k in q_norm for k in ["bao nhiêu", "tín chỉ", "chương trình", "ngành", "clc", "quy chế"]):
        use_rag = True

    # 2. retrieve nếu cần
    context = ""
    if use_rag:
        context = await retrieve_graph_context(rewritten_question)
    logger.info(
        "[chat][context] use_rag=%s context_len=%d context=%s",
        str(use_rag),
        len(context or ""),
        _truncate_text(context, 2500),
    )

    # 3. build prompt
    prompt = build_prompt(rewritten_question, context, clean_history)

    # 4. chọn model
    try:
        if complexity == "simple" and not use_rag:
            logger.info("[chat][model] selected=3b")
            answer = await call_model_3b(prompt)
        else:
            logger.info("[chat][model] selected=7b")
            answer = await call_model_7b(prompt)
    except Exception:
        logger.exception("[chat][model] Response generation failed")
        if use_rag and context:
            return "Không tìm thấy thông tin."
        return "Hệ thống tạm thời không gọi được mô hình sinh phản hồi. Vui lòng thử lại sau ít phút."

    answer = (answer or "").strip()

    # Post-guard: nếu cần dữ liệu nhưng context rỗng thì trả fallback thống nhất.
    if use_rag and not context and (not answer or re.search(r"không rõ|khó xác định|có thể", answer.lower())):
        logger.info("[chat][result] fallback=khong_tim_thay_thong_tin reason=empty_context_or_uncertain_answer")
        return "Không tìm thấy thông tin."

    logger.info("[chat][result] answer=%s", _truncate_text(answer, 1500))
    return answer or "Không tìm thấy thông tin."
