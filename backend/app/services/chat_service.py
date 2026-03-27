import json
import logging
import re
from typing import Any, Dict, List

from app.services.llm_service import call_model_7b
from app.services.retrieval_service import retrieve_graph_context

logger = logging.getLogger("app.chat")

def _truncate_text(text: str, limit: int = 1200) -> str:
    """Cắt bớt text dài để tránh tràn log hoặc context."""
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "...<truncated>"

def _sanitize_history(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Làm sạch và giữ lại 8 tin nhắn gần nhất để duy trì ngữ cảnh."""
    cleaned = []
    for msg in (history or [])[-8:]:
        role = str(msg.get("role", "user")).strip().lower()
        content = str(msg.get("content", "")).strip()
        if role not in ["user", "assistant"]:
            role = "user"
        if content:
            cleaned.append({"role": role, "content": content})
    return cleaned

async def analyze_and_route(question: str, history: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Sử dụng model 7B làm Router:
    1. Viết lại câu hỏi (kế thừa thực thể từ lịch sử).
    2. Xác định xem có cần truy vấn Graph/Vector DB hay không (thay thế bước classify cũ).
    """
    # Nếu câu hỏi quá ngắn mà mang tính xác nhận, giữ nguyên
    if len(question.strip()) < 3 or question.strip().lower() in ["có", "đúng", "ok", "không"]:
        return {"rewritten_query": question, "needs_rag": False}

    history_text = "\n".join([f"{m['role']}: {m['content']}" for m in history[-4:]])
    
    prompt = f"""
Bạn là bộ não điều phối (Router) của Chatbot Tư vấn đào tạo Đại học Cần Thơ (CTU).
Hệ thống cơ sở dữ liệu của bạn chứa các thông tin về: Chương trình đào tạo, Môn học (Học phần), Tín chỉ, Ngành học, Khoa, Chuẩn đầu ra, Vị trí việc làm, và Văn bản pháp lý quy chế.

Nhiệm vụ của bạn:
1. "rewritten_query": Viết lại câu hỏi của người dùng cho thật rõ nghĩa, tự đóng gói (self-contained). Nếu người dùng dùng đại từ "nó", "ngành đó", hãy thay thế bằng tên thực thể cụ thể có trong Lịch sử hội thoại.
2. "needs_rag": Trả về `true` nếu câu hỏi liên quan đến tra cứu thông tin học vụ, quy chế, điểm số, chương trình đào tạo như mô tả ở trên. Trả về `false` nếu chỉ là câu hỏi xã giao, cảm ơn, hỏi thăm sức khỏe hoặc câu hỏi kiến thức chung không liên quan đến đại học.

Lịch sử hội thoại gần đây:
{history_text if history_text else "Không có"}

Câu hỏi hiện tại của người dùng: "{question}"

Bạn CHỈ ĐƯỢC PHÉP trả về kết quả dưới dạng JSON hợp lệ, không giải thích gì thêm:
{{
  "rewritten_query": "câu hỏi đã viết lại",
  "needs_rag": true hoặc false
}}
"""
    try:
        # Temperature 0.6 cho JSON generation - cần balance giữa creativity và structure
        response = await call_model_7b(prompt, temperature=0.6)
        # Trích xuất đoạn JSON từ phản hồi đề phòng model trả thêm text rác
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        logger.error(f"[chat][route] Lỗi khi phân tích và định tuyến: {e}")
    
    # Fallback an toàn: Nếu lỗi xử lý, mặc định cần RAG để không bỏ sót yêu cầu
    return {"rewritten_query": question, "needs_rag": True}

def build_final_prompt(question: str, context: str, history: List[Dict[str, str]]) -> str:
    """Xây dựng Prompt cuối cùng để sinh câu trả lời."""
    history_text = "\n".join([f"{m['role']}: {m['content']}" for m in history]) if history else ""
    
    has_context = bool(context and context.strip())
    
    if has_context:
        # When data is available: Force model to use it
        return f"""Bạn là Trợ lý tư vấn đào tạo của Đại học Cần Thơ (CTU).

BƯỚC 1: ĐỌC DỮ LIỆU BẮTBUỘC
Dữ liệu dưới đây là DỮ LIỆU CHÍNH THỨC từ cơ sở dữ liệu Neo4j CTU. BẠN PHẢI DÙNG dữ liệu này để trả lời.

DỮ LIỆU TỪ CTU:
{context}

BƯỚC 2: TRÍCH XUẤT TỪ DỮ LIỆU
- Đọc từng dòng dữ liệu trên.
- Tóm tắt và liệt kê thông tin cụ thể từ dữ liệu.
- KHÔNG bịa chuyện hoặc thêm thông tin ngoài dữ liệu.

BƯỚC 3: TRẢ LỜI
Câu hỏi: {question}
Câu trả lời (PHẢI dùng dữ liệu trên):"""
    else:
        # When data is empty: Allow fallback
        return f"""Bạn là Trợ lý tư vấn đào tạo của Đại học Cần Thơ (CTU).

Lịch sử:
{history_text if history_text else "(Trống)"}

Dữ liệu từ hệ thống: [Không có dữ liệu phù hợp]

Câu hỏi: {question}

Câu trả lời: Tôi không tìm thấy thông tin này trong hệ thống."""

async def chat_handler(question: str, history: List[Dict[str, Any]]):
    """Pipeline chính xử lý yêu cầu chat."""
    
    clean_history = _sanitize_history(history or [])
    logger.info(f"[chat][request] question={_truncate_text(question, 200)} history_len={len(clean_history)}")

    # --- BƯỚC 1: FAST-TRACK (Ngắt mạch cho câu xã giao) ---
    q_norm = question.strip().lower()
    social_keywords = ["chào", "hi", "hello", "cảm ơn", "thanks", "tạm biệt", "bye", "chúc ngủ ngon"]
    if len(q_norm) < 15 and any(kw in q_norm for kw in social_keywords):
        logger.info("[chat][fast-track] Phát hiện câu hỏi xã giao ngắn.")
        fast_prompt = f"Người dùng nói: '{question}'. Bạn là chatbot của Đại học Cần Thơ, hãy trả lời xã giao thật ngắn gọn, thân thiện."
        try:
            # Temperature thấp cho greeting (0.4)
            return await call_model_7b(fast_prompt, temperature=0.4)
        except Exception:
            return "Chào bạn! Mình có thể giúp gì cho bạn về chương trình đào tạo của CTU?"

    # --- BƯỚC 2: ROUTER & REWRITE (Sử dụng 7B) ---
    analysis = await analyze_and_route(question, clean_history)
    rewritten_query = analysis.get("rewritten_query", question)
    needs_rag = analysis.get("needs_rag", True)
    
    logger.info(f"[chat][router] needs_rag={needs_rag} rewritten={_truncate_text(rewritten_query, 200)}")

    # --- BƯỚC 3: RETRIEVAL (Truy xuất Neo4j/Vector nếu cần) ---
    context = ""
    if needs_rag:
        try:
            context = await retrieve_graph_context(rewritten_query)
            logger.info(f"[chat][retrieval] Lấy được context: {context}")
        except Exception as e:
            logger.error(f"[chat][retrieval] Lỗi khi lấy context: {e}")

    # --- BƯỚC 4: SINH CÂU TRẢ LỜI (Sử dụng 7B) ---
    final_prompt = build_final_prompt(rewritten_query, context, clean_history)
    
    try:
        logger.info("[chat][model] Đang gọi model 7B để sinh câu trả lời...")
        # Temperature rất thấp (0.2) để model tập trung 100% vào data và tuân thủ instruction
        answer = await call_model_7b(final_prompt, temperature=0.2)
        answer = (answer or "").strip()
    except Exception as e:
        logger.exception("[chat][model] Lỗi gọi mô hình sinh phản hồi")
        return "Hệ thống tạm thời không phản hồi được. Vui lòng thử lại sau ít phút."

    # --- BƯỚC 5: POST-GUARD CHECK (Kiểm tra chặn ảo giác) ---
    # Nếu cần RAG nhưng không có context, và mô hình cố tình trả lời dài dòng/mơ hồ thay vì từ chối.
    if needs_rag and not context:
        uncertain_patterns = ["có thể", "thường thì", "tôi nghĩ", "khó xác định", "không chắc"]
        if not answer or any(p in answer.lower() for p in uncertain_patterns):
            logger.info("[chat][guard] Fallback được kích hoạt do thiếu context và câu trả lời mơ hồ.")
            return "Xin lỗi, hiện tại tôi không tìm thấy thông tin chính xác về vấn đề này trong hệ thống quy chế đào tạo."

    logger.info(f"[chat][result] answer={_truncate_text(answer, 200)}")
    return answer or "Không tìm thấy thông tin."