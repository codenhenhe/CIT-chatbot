import httpx
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv


def _load_env_files() -> None:
    # Load backend and repo-level env files to support both dev run locations.
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

logger = logging.getLogger("app.llm")
_loaded_models: set[str] = set()


def _mark_model_used(model: str) -> None:
    name = (model or "").strip()
    if name:
        _loaded_models.add(name)


def _extract_json_object(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}

    # Try parse raw content first.
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # Try parse fenced code or inline JSON object.
    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        return {}

    try:
        data = json.loads(match.group(0))
        if isinstance(data, dict):
            return data
    except Exception:
        return {}

    return {}


async def call_model(model: str, prompt: str, temperature: float = 0.3, keep_alive: str = "1h"):
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
        try:
            # Preferred endpoint for prompt-style calls.
            res = await client.post(
                OLLAMA_GENERATE_URL,
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "temperature": temperature,
                    "top_p": 0.9,
                    "top_k": 40,
                    "keep_alive": keep_alive,
                },
            )
            if res.status_code == 404:
                raise httpx.HTTPStatusError("Generate endpoint not found", request=res.request, response=res)
            res.raise_for_status()
            payload = res.json()
            if "response" not in payload:
                raise RuntimeError(f"Invalid Ollama response: {payload}")
            _mark_model_used(model)
            return payload["response"]
        except httpx.HTTPStatusError as e:
            # Fallback for runtimes exposing /api/chat but not /api/generate.
            if e.response is not None and e.response.status_code == 404:
                chat_res = await client.post(
                    OLLAMA_CHAT_URL,
                    json={
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "temperature": temperature,
                        "top_p": 0.9,
                    },
                )
                chat_res.raise_for_status()
                chat_payload = chat_res.json()
                content = (chat_payload.get("message", {}) or {}).get("content")
                if not content:
                    raise RuntimeError(f"Invalid Ollama chat response: {chat_payload}")
                _mark_model_used(model)
                return content
            raise


async def call_model_json(model: str, prompt: str) -> Dict[str, Any]:
    text = await call_model(model, prompt, temperature=0.3)
    return _extract_json_object(text)


async def call_model_3b(prompt: str):
    # Backward-compatible alias kept for existing imports.
    return await call_model(MODEL_AUX_4B, prompt, temperature=0.3)


async def call_model_7b(prompt: str, temperature: float = 0.3):
    return await call_model(MODEL_AUX_7B, prompt, temperature=temperature)


async def call_model_3b_json(prompt: str) -> Dict[str, Any]:
    # Backward-compatible alias kept for existing imports.
    return await call_model_json(MODEL_AUX_4B, prompt)


async def call_model_4b_json(prompt: str) -> Dict[str, Any]:
    return await call_model_json(MODEL_AUX_4B, prompt)


async def call_model_9b(prompt: str, temperature: float = 0.3):
    return await call_model(MODEL_PRIMARY_9B, prompt, temperature=temperature)


async def warmup_llm_model() -> bool:
    """
    Khởi động model 9b khi backend startup và giữ nó trong GPU memory.
    Gửi một query test với keep_alive cao để ép Ollama giữ model.
    """
    logger.info("[llm] Bắt đầu warmup model 9b...")
    try:
        # Test prompt tối thiểu để trigger model load
        # keep_alive="1h" báo cho Ollama giữ model trong 1 giờ
        warmup_prompt = "Xin chào"
        response = await call_model(
            MODEL_PRIMARY_9B, 
            warmup_prompt, 
            temperature=0.1,
            keep_alive="1h"  # Giữ model trong 1 giờ sau warmup
        )
        if response:
            logger.info(f"[llm] Warmup model 9b thành công! Model kept alive. Response length: {len(response)}")
            return True
        else:
            logger.warning("[llm] Warmup model 9b nhận response rỗng")
            return False
    except Exception as e:
        logger.error(f"[llm] Lỗi khi warmup model 9b: {e}")
        return False


async def unload_llm_models() -> bool:
    """
    Giải phóng models khi backend shutdown.
    Gửi request với keep_alive=0 để báo cho Ollama unload model từ GPU.
    """
    logger.info("[llm] Bắt đầu unload LLM models...")
    models_to_unload = sorted(_loaded_models)
    if not models_to_unload:
        logger.info("[llm] Không có model nào đã dùng trong runtime, bỏ qua unload")
        return True
    
    async with httpx.AsyncClient(timeout=10) as client:
        for model in models_to_unload:
            try:
                # Gửi request với keep_alive=0 để force unload model
                await client.post(
                    OLLAMA_GENERATE_URL,
                    json={
                        "model": model,
                        "prompt": " ",  # Minimal valid prompt
                        "stream": False,
                        "keep_alive": 0,  # Force unload after this request
                    },
                    timeout=5,
                )
                logger.info(f"[llm] Unload signal sent for {model}")
            except Exception as e:
                logger.warning(f"[llm] Couldn't unload {model}: {e}")
    
    logger.info("[llm] Unload completed")
    return True


class LLMService:
    """High-level LLM wrapper used by orchestration services."""

    def __init__(self) -> None:
        self.cache_ttl_seconds = int(os.getenv("LLM_CACHE_TTL_SECONDS", "120"))
        self.fast_routing = os.getenv("FAST_ROUTING", "true").strip().lower() in {"1", "true", "yes"}
        self._cache: Dict[str, tuple[float, Any]] = {}

    def _cache_key(self, method: str, payload: Dict[str, Any]) -> str:
        return json.dumps({"method": method, "payload": payload}, ensure_ascii=False, sort_keys=True)

    def _get_cache(self, key: str) -> Any:
        item = self._cache.get(key)
        if not item:
            return None
        ts, value = item
        if (time.time() - ts) > self.cache_ttl_seconds:
            self._cache.pop(key, None)
            return None
        return value

    def _set_cache(self, key: str, value: Any) -> None:
        self._cache[key] = (time.time(), value)
        if len(self._cache) > 1000:
            now = time.time()
            expired = [k for k, (ts, _) in self._cache.items() if (now - ts) > self.cache_ttl_seconds]
            for k in expired:
                self._cache.pop(k, None)

    async def should_query(self, query: str, history: str = "") -> Dict[str, Any]:
        cache_key = self._cache_key("should_query", {"query": query, "history": history[-500:]})
        cached = self._get_cache(cache_key)
        if cached is not None:
            logger.info("[llm][cache] hit should_query")
            return cached

        prompt = (
            "Bạn là bộ điều phối truy vấn cho chatbot CTDT. "
            "Hãy quyết định có cần truy vấn dữ liệu (Neo4j/Vector) hay không. "
            "need_query=true khi câu hỏi cần tra cứu dữ liệu học vụ/ctdt/quy định cụ thể. "
            "need_query=false khi câu hỏi xã giao, cảm ơn, chào hỏi, hoặc hội thoại không cần dữ liệu. "
            "Chỉ trả về JSON {\"need_query\": true/false, \"reason\": \"...\"}."
            f"\nHistory: {history or '[empty]'}"
            f"\nQuery: {query}"
        )
        data = await call_model_json(MODEL_AUX_7B, prompt)
        if not data:
            data = await call_model_json(MODEL_AUX_4B, prompt)
        if not data:
            return {"need_query": True, "reason": "fallback_true"}

        result = {
            "need_query": bool(data.get("need_query", True)),
            "reason": str(data.get("reason", "")).strip() or "llm_decision",
        }
        self._set_cache(cache_key, result)
        return result

    async def generate_without_query(self, query: str, history: str = "") -> str:
        prompt = f"""
            Bạn là trợ lý tư vấn đào tạo thân thiện và ngắn gọn cho Đại học Cần Thơ (CTU).

            ## Nhiệm vụ
            Trả lời các câu KHÔNG cần truy vấn dữ liệu, ví dụ:
            - chào hỏi
            - cảm ơn
            - hỏi bạn là ai
            - hỏi khả năng của hệ thống
            - câu hỏi chung chung không yêu cầu thông tin cụ thể

            ## Nguyên tắc
            - Trả lời tự nhiên, thân thiện, ngắn gọn (1–3 câu)
            - KHÔNG bịa thông tin chuyên môn (quy chế, môn học, tín chỉ...)
            - Nếu câu hỏi cần dữ liệu cụ thể → nói rõ rằng cần kiểm tra dữ liệu
            - Không suy diễn hoặc tự tạo thông tin

            ## History (nếu có)
            {history or "[empty]"}

            ## User
            {query}

            ## Assistant
        """
        answer = await call_model_9b(prompt, temperature=0.4)
        return (answer or "").strip()

    async def rewrite(self, query: str, feedback: str = "") -> str:
        prompt = f"""
            Bạn là hệ thống REWRITE truy vấn cho chatbot chương trình đào tạo.

            ## Nhiệm vụ
            Viết lại câu hỏi của người dùng sao cho:
            - Ngắn gọn
            - Rõ ràng
            - Dễ hiểu hơn cho hệ thống truy vấn

            ## Nguyên tắc BẮT BUỘC
            - GIỮ NGUYÊN ý nghĩa câu hỏi
            - KHÔNG thêm thông tin mới
            - KHÔNG suy diễn
            - PHẢI giữ nguyên các thực thể quan trọng:
            - mã môn (ví dụ: INT2201)
            - tên môn
            - ngành học
            - KHÔNG được làm mất hoặc thay đổi các thực thể này

            ## Khi có feedback từ hệ thống:
            {f"- {feedback}" if feedback else "- Không có"}

            Hãy sửa câu hỏi để khắc phục vấn đề trong feedback (nếu có).

            ## Output
            - Chỉ trả về DUY NHẤT câu hỏi đã viết lại
            - Không giải thích

            ## Câu hỏi gốc
            {query}

            ## Câu hỏi viết lại
        """
        rewritten = await call_model_9b(prompt, temperature=0.2)
        return (rewritten or query).strip()

    async def generate(self, query: str, context: str) -> str:
        prompt = f"""
            Bạn là trợ lý tư vấn đào tạo Đại học Cần Thơ (CTU).

            ## Nhiệm vụ
            Trả lời câu hỏi của người dùng CHỈ dựa trên thông tin trong Context.

            ## Nguyên tắc BẮT BUỘC
            - CHỈ sử dụng thông tin có trong Context
            - KHÔNG thêm kiến thức bên ngoài
            - KHÔNG suy đoán
            - Nếu Context KHÔNG đủ:
                + Nói rõ là chưa đủ thông tin
                + Gợi ý người dùng hỏi rõ hơn

            ## Cách trả lời
            - Trả lời trực tiếp vào câu hỏi
            - Nếu có nhiều ý → dùng bullet

            ## Context
            {context or "[Không có context]"}

            ## Câu hỏi
            {query}

            ## Trả lời
        """
        answer = await call_model_9b(prompt, temperature=0.2)
        return (answer or "").strip()

    async def classify_domain(self, query: str) -> str:
        cache_key = self._cache_key("classify_domain", {"query": query})
        cached = self._get_cache(cache_key)
        if cached is not None:
            logger.info("[llm][cache] hit classify_domain")
            return str(cached)

        prompt = f"""
            Phân loại domain của câu hỏi thành 1 trong 2:
            - ctdt: hỏi về môn học, chương trình đào tạo, ngành, tín chỉ
            - quy_che: hỏi về quy định, điều kiện, có được hay không

            Nguyên tắc:
            - Chỉ chọn 1 domain
            - Ưu tiên "quy_che" nếu câu hỏi liên quan đến điều kiện hoặc quy định

            Output JSON:
            {{"domain": "ctdt | quy_che"}}

            Query: {query}
        """
        model_order = [MODEL_AUX_4B, MODEL_AUX_7B, MODEL_PRIMARY_9B] if self.fast_routing else [MODEL_PRIMARY_9B, MODEL_AUX_7B, MODEL_AUX_4B]
        data: Dict[str, Any] = {}
        for model in model_order:
            data = await call_model_json(model, prompt)
            if data:
                break
        result = str(data.get("domain", "ctdt")).strip().lower()
        self._set_cache(cache_key, result)
        return result

    async def classify_intent(self, query: str) -> str:
        cache_key = self._cache_key("classify_intent", {"query": query})
        cached = self._get_cache(cache_key)
        if cached is not None:
            logger.info("[llm][cache] hit classify_intent")
            return str(cached)

        prompt = f"""
            Bạn là hệ thống phân loại intent cho chatbot giáo dục.

            ## Nhiệm vụ
            Phân loại câu hỏi của người dùng vào 1 trong 3 loại:

            ### 1. factual
            - Hỏi thông tin cụ thể, trực tiếp
            - Không yêu cầu suy luận nhiều

            Ví dụ:
            - "Môn AI có bao nhiêu tín chỉ?"
            - "INT2201 là môn gì?"

            ### 2. relational
            - Hỏi về mối quan hệ giữa các thực thể
            - Ví dụ: tiên quyết, thuộc ngành, liên quan

            Ví dụ:
            - "Môn nào là tiên quyết của AI?"
            - "AI thuộc ngành nào?"

            ### 3. rule
            - Hỏi về quy định, điều kiện, có được hay không
            - Thường chứa: "có được", "khi nào", "điều kiện"

            Ví dụ:
            - "Có được học khi thiếu tiên quyết không?"
            - "Khi nào bị cảnh cáo học vụ?"

            ## Nguyên tắc
            - Chỉ chọn 1 loại DUY NHẤT
            - Ưu tiên "rule" nếu câu hỏi liên quan đến điều kiện hoặc quy định
            - Không giải thích

            ## Output
            Chỉ trả về JSON đúng format:
            {{"intent": "factual | relational | rule"}}

            ## Query
            {query}
        """
        model_order = [MODEL_AUX_7B, MODEL_AUX_4B, MODEL_PRIMARY_9B] if self.fast_routing else [MODEL_PRIMARY_9B, MODEL_AUX_7B, MODEL_AUX_4B]
        data: Dict[str, Any] = {}
        for model in model_order:
            data = await call_model_json(model, prompt)
            if data:
                break
        result = str(data.get("intent", "factual")).strip().lower()
        self._set_cache(cache_key, result)
        return result

    async def analyze_all_in_one(self, query: str, history: str = "") -> Dict[str, Any]:
        prompt = f"""
            Bạn là chuyên gia phân tích ngôn ngữ cho Chatbot Đại học Cần Thơ (CTU).
            Nhiệm vụ: Chuyển đổi câu hỏi người dùng thành dữ liệu cấu trúc JSON để truy vấn Database.

            ### 1. BẢNG TRA CỨU VIẾT TẮT (BẮT BUỘC SỬ DỤNG):
            Khi viết lại câu hỏi (rewritten_query), PHẢI chuyển từ viết tắt sang tên đầy đủ sau:
            - KHMT -> Khoa học máy tính
            - KTPM -> Kỹ thuật phần mềm
            - CNTT -> Công nghệ thông tin
            - HTTT -> Hệ thống thông tin
            - MMT & TTDL -> Mạng máy tính và Truyền thông dữ liệu
            - ATTT -> An toàn thông tin
            - TTNT -> Trí tuệ nhân tạo
            - KHDL -> Khoa học dữ liệu
            - TC/Tín -> Tín chỉ
            - HP -> Học phần
            - CTDT -> Chương trình đào tạo
            - QCHV -> Quy chế học vụ
            - HK -> Học kỳ

            ### 2. PHÂN LOẠI INTENT:
            - factual: Hỏi thông số cụ thể (VD: "Môn A mấy tín?", "Tổng tín chỉ ngành B").
            - relational: Hỏi về sự liên kết (VD: "Môn tiên quyết của A", "Môn B thuộc khối nào").
            - rule: Hỏi về quy định/điều kiện (VD: "Điều kiện xét tốt nghiệp", "Cách đăng ký học cải thiện").

            ### 3. YÊU CẦU CHO REWRITTEN_QUERY:
            - PHẢI sử dụng Tiếng Việt chuẩn, có dấu.
            - PHẢI sử dụng tên ngành/môn học ĐẦY ĐỦ theo bảng viết tắt trên để khớp với Database.
            - KHÔNG được thay đổi ý nghĩa gốc của người dùng.

            ### 4. QUY TẮC QUYẾT ĐỊNH TRUY VẤN (need_query):
            - need_query = false: Khi câu hỏi là xã giao, chào hỏi, cảm ơn, khen ngợi hoặc các câu nói không chứa thực thể học vụ (VD: "Chào bot", "Bạn khỏe không?", "Cảm ơn nhé").
            - need_query = true: Khi câu hỏi cần thông tin về môn học, ngành học, chương trình đào tạo, quy chế học vụ hoặc bất kỳ thực thể nào của CTU.

            ### 5. PHÂN LOẠI DOMAIN (QUY TẮC QUYẾT ĐỊNH):
            - "ctdt": Khi câu hỏi tập trung vào THỰC THỂ CỤ THỂ của một ngành hoặc môn học.
                + Dấu hiệu: Tên ngành (KHMT, CNTT...), Mã học phần (CT101, CT173...), Tên môn học, Tín chỉ của môn, Môn tiên quyết, Mô tả học phần, Kế hoạch học tập từng kỳ của ngành.
            - "quy_che": Khi câu hỏi tập trung vào QUY ĐỊNH CHUNG áp dụng cho toàn bộ sinh viên hoặc quy trình hành chính.
                + Dấu hiệu: Đăng ký học phần, học cải thiện, học bù, cảnh báo học vụ, xét tốt nghiệp (điều kiện chung), thang điểm, xếp loại học lực, các chứng chỉ ngoại ngữ/tin học bắt buộc (chuẩn đầu ra chung).

            DỮ LIỆU ĐẦU VÀO:
            - Lịch sử chat: {history[-500:]}
            - Câu hỏi hiện tại: "{query}"

            YÊU CẦU OUTPUT JSON (DUY NHẤT):
            {{
                "domain": "ctdt" | "quy_che",
                "intent": "factual" | "relational" | "rule",
                "entities": {{ "mentions": ["tên đầy đủ thực thể"], "intent_hint": "từ khóa hành động" }},
                "rewritten_query": "câu hỏi đã được chuẩn hóa tên đầy đủ",
                "need_query": true/false
            }}
        """
        return await call_model_json(MODEL_PRIMARY_9B, prompt)

    async def call_model_4b_json(self, prompt: str) -> Dict[str, Any]:
        """Compatibility wrapper for services expecting instance-level JSON calls."""
        return await call_model_4b_json(prompt)

    async def extract_entities(self, query: str) -> Dict[str, Any]:
        cache_key = self._cache_key("extract_entities", {"query": query})
        cached = self._get_cache(cache_key)
        if cached is not None:
            logger.info("[llm][cache] hit extract_entities")
            return cached

        prompt = f"""
            Bạn là hệ thống phân tích câu hỏi cho chatbot đào tạo.

            ## Nhiệm vụ
            Trích xuất thông tin quan trọng từ câu hỏi gồm:
            - mentions
            - intent_hint
            - relation

            1. mentions:
            - Các thực thể xuất hiện trong câu (giữ nguyên text)

            2. intent_hint:
            - Người dùng đang hỏi gì?
            - Ví dụ: "tín chỉ", "tiên quyết", "học lại", "điều kiện", "thuộc ngành"

            3. relation (nếu có):
            - Mối quan hệ giữa các thực thể
            - Ví dụ:
            - "prerequisite"
            - "belong_to"
            - "requirement"

            ## Nguyên tắc
            - KHÔNG suy đoán
            - CHỈ dùng thông tin có trong câu
            - Nếu không có:
            - mentions = []
            - intent_hint = ""
            - relation = null

            ## Output
            JSON:
            {{
            "mentions": ["..."],
            "intent_hint": "...",
            "relation": "..." 
            }}

            ## Query
            {query}
        """

        data = await call_model_json(MODEL_PRIMARY_9B, prompt)
        if not data:
            data = await call_model_json(MODEL_AUX_7B, prompt)
        if not data:
            data = await call_model_4b_json(prompt)

        # fallback nếu model trả lỗi
        if not isinstance(data, dict):
            data = {}

        # validate + normalize
        mentions = data.get("mentions", [])
        if not isinstance(mentions, list):
            mentions = []

        mentions = [str(m).strip() for m in mentions if str(m).strip()]

        intent_hint = data.get("intent_hint", "")
        if not isinstance(intent_hint, str):
            intent_hint = ""
        intent_hint = intent_hint.strip()

        relation = data.get("relation")
        if not isinstance(relation, str):
            relation = None
        else:
            relation = relation.strip() or None

        result: Dict[str, Any] = {
            "mentions": mentions,
            "intent_hint": intent_hint,
            "relation": relation,
        }
        self._set_cache(cache_key, result)
        return result