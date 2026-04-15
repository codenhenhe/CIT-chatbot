import logging
import os
import re
import requests
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger("app.quyche_llm")


def _load_env_files() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    repo_root = backend_root.parent
    load_dotenv(backend_root / ".env", override=False)
    load_dotenv(repo_root / ".env", override=False)


def _get_env(*keys: str, default: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default


_load_env_files()

OLLAMA_BASE_URL = _get_env("OLLAMA_BASE_URL", "OLLAMA_HOST", default="http://localhost:11434").rstrip("/")
OLLAMA_GENERATE_URL = os.getenv("OLLAMA_GENERATE_URL", f"{OLLAMA_BASE_URL}/api/generate")
OLLAMA_CHAT_URL = os.getenv("OLLAMA_CHAT_URL", f"{OLLAMA_BASE_URL}/api/chat")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180"))
MODEL_PRIMARY_9B = _get_env("QWEN3_5_MODEL_9B", "OLLAMA_MODEL_9B", default="qwen3.5:9b")
MODEL_AUX_7B = _get_env("QWEN2_5_MODEL_7B", "OLLAMA_MODEL_7B", default="qwen2.5-coder:7b-instruct")
MODEL_AUX_4B = _get_env("QWEN3_5_MODEL_4B", "OLLAMA_MODEL_4B", default="qwen3.5:4b")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "BAAI/bge-m3")

VANBAN_MAP = {
    "quychehocvu":            "quychehocvu",
    "quydinhdaotaotructuyen": "quydinhdaotaotructuyen",
    "qcdt_vhvl":              "QCDTVHVL",
    "qcdt_tuxa":              "QCDTTuXa",
}

VANBAN_DISPLAY = {
    "quychehocvu":            "Quy chế Học vụ",
    "quydinhdaotaotructuyen": "Quy định Đào tạo Trực tuyến",
    "QCDTVHVL":               "Quy chế Đào tạo Vừa học vừa làm",
    "QCDTTuXa":               "Quy chế Đào tạo Từ xa",
}

VANBAN_KEYWORDS = {
    "quydinhdaotaotructuyen": ["trực tuyến", "online", "e-learning", "đào tạo trực tuyến"],
    "qcdt_vhvl":              ["vừa học vừa làm", "vhvl", "tại chức", "hệ vừa học"],
    "qcdt_tuxa":              ["từ xa", "đào tạo từ xa", "hệ từ xa"],
}

VANBAN_CLASSIFY_SYSTEM = """Phân loại câu hỏi vào đúng văn bản quy chế CTU.
1. quychehocvu       — Quy chế Học vụ (hệ chính quy)
2. quydinhdaotaotructuyen — Đào tạo Trực tuyến / online / e-learning
3. qcdt_vhvl         — Đào tạo Vừa học vừa làm / tại chức
4. qcdt_tuxa         — Đào tạo Từ xa

Chỉ trả về 1 trong 4 giá trị trên, không giải thích."""

PREPROCESS_SYSTEM = """Bạn là công cụ CHUẨN HÓA văn bản, KHÔNG phải trợ lý giải đáp.
Nhiệm vụ:
1. Sửa lỗi chính tả, gõ phím, thiếu dấu tiếng Việt.
2. CHỈ đổi số sau từ "Chương" sang số La Mã. VD: "chương 1" → "Chương I".
3. GIỮ NGUYÊN số sau Điều và Khoản (số Ả-rập). VD: "Điều 9", "Khoản 1".
4. GIỮ NGUYÊN chữ cái sau Điểm. VD: "Điểm a".
5. KHÔNG tự ý thay đổi ý nghĩa câu hỏi.
Chỉ trả về đúng 1 dòng là câu đã chuẩn hóa."""

CHITCHAT_SYSTEM = """Bạn là bộ phân loại câu hỏi.
Xác định câu có phải xã giao/chào hỏi không (chào, cảm ơn, hỏi tên bot, không liên quan quy chế).
Chỉ trả về: YES nếu là xã giao, NO nếu không phải."""

CHITCHAT_RESPONSE_SYSTEM = """Bạn là chatbot hỗ trợ sinh viên Trường Đại học Cần Thơ (CTU).
Trả lời ngắn gọn, thân thiện bằng tiếng Việt.
Nhắc nhở bạn có thể hỏi về quy chế học vụ, quy định đào tạo của trường."""

ANSWER_SYSTEM = """Bạn là chatbot hỗ trợ sinh viên Trường Đại học Cần Thơ (CTU).
Trả lời câu hỏi dựa trên NỘI DUNG ĐƯỢC CUNG CẤP.
- Trả lời rõ ràng, thân thiện, đúng trọng tâm bằng tiếng Việt.
- Không bịa đặt thông tin ngoài nội dung được cung cấp.
- Nếu không đủ thông tin, nói rõ và khuyên liên hệ phòng Đào tạo.
- Luôn nhắc tên quy chế/văn bản nguồn nếu có."""


class QuyCheLLMService:
    def __init__(self) -> None:
        self.model_primary = MODEL_PRIMARY_9B
        self.timeout_seconds = DEFAULT_TIMEOUT_SECONDS

    def _call_llm(self, messages: list, temperature: float = 0.1) -> str:
        url = OLLAMA_CHAT_URL
        payload = {
            "model": self.model_primary,
            "messages": messages,
            "stream": False,
            "options": {"temperature": temperature},
        }
        try:
            resp = requests.post(url, json=payload, timeout=self.timeout_seconds)
            resp.raise_for_status()
            return resp.json()["message"]["content"].strip()
        except Exception as e:
            logger.error(f"[QuyCheLLM] Lỗi gọi Ollama: {e}")
            return ""

    def preprocess_question(self, raw: str) -> str:
        msgs = [
            {"role": "system", "content": PREPROCESS_SYSTEM},
            {"role": "user",   "content": raw.strip()},
        ]
        result = self._call_llm(msgs, temperature=0.0)
        if not result or len(result) > 500:
            return raw
        return result.strip()

    def is_chitchat(self, question: str) -> bool:
        q = question.lower()
        return any(x in q for x in [
            "chào", "hi", "hello", "xin chào", "hey",
            "bạn là ai", "tên gì", "cảm ơn", "thanks"
        ])

    def answer_chitchat(self, question: str, history: list) -> str:
        msgs = (
            [{"role": "system", "content": CHITCHAT_RESPONSE_SYSTEM}]
            + history[-6:]
            + [{"role": "user", "content": question}]
        )
        return self._call_llm(msgs, temperature=0.5)

    def detect_vanban(self, question: str) -> str:
        q_lower = question.lower()
        for vb_key, keywords in VANBAN_KEYWORDS.items():
            for kw in keywords:
                if kw in q_lower:
                    return VANBAN_MAP[vb_key]
        msgs = [
            {"role": "system", "content": VANBAN_CLASSIFY_SYSTEM},
            {"role": "user",   "content": question},
        ]
        raw_out = self._call_llm(msgs, temperature=0.0).strip().lower()
        for key in VANBAN_MAP:
            if key in raw_out:
                return VANBAN_MAP[key]
        return VANBAN_MAP["quychehocvu"]

    def generate_answer(self, question: str, context: str, history: list, vb_display: str = "") -> str:
        history_msgs = history[-6:]
        source_note = f"\nNguồn: {vb_display}" if vb_display else ""
        user_content = (
            f"Câu hỏi: {question}\n\n"
            f"Nội dung từ văn bản quy chế:{source_note}\n"
            "---\n"
            f"{context if context else '(Không tìm thấy nội dung liên quan — hãy thông báo cho sinh viên liên hệ phòng Đào tạo)'}\n"
            "---\n\n"
            "Hãy trả lời câu hỏi trên."
        )
        messages = (
            [{"role": "system", "content": ANSWER_SYSTEM}]
            + history_msgs
            + [{"role": "user", "content": user_content}]
        )
        return self._call_llm(messages, temperature=0.2)


quyche_llm_service = QuyCheLLMService()
