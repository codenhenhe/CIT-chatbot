import os
import json
import re
import unicodedata
import time
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import ollama
import asyncio

# Import các class đã xây dựng
from app.scripts.Embedding import EmbeddingModel
from app.scripts.neo4j_class import Neo4jConnector

# Nạp cấu hình từ .env
load_dotenv("../.env")


class TTLCache:
    def __init__(self, ttl_seconds: int = 900, max_size: int = 1000):
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._store: Dict[str, Any] = {}

    def _purge(self):
        now = time.monotonic()
        expired = [k for k, (_, exp) in self._store.items() if exp <= now]
        for k in expired:
            self._store.pop(k, None)

        overflow = len(self._store) - self.max_size
        if overflow > 0:
            oldest = sorted(self._store.items(), key=lambda item: item[1][1])[:overflow]
            for k, _ in oldest:
                self._store.pop(k, None)

    def get(self, key: str):
        item = self._store.get(key)
        if not item:
            return None
        value, exp = item
        if exp <= time.monotonic():
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any):
        self._purge()
        self._store[key] = (value, time.monotonic() + self.ttl_seconds)

    def size(self) -> int:
        self._purge()
        return len(self._store)

class GraphRAGChatbot:
    def __init__(self):
        print("--- Đang khởi tạo Embedding Model (BGE-M3)... ---")
        self.embed_service = EmbeddingModel(
            model_name="BAAI/bge-m3"
        )
        print(f"[Startup] Embedding runtime device: {self.embed_service.device}")
        
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USERNAME", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        
        print("--- Đang kết nối tới Neo4j... ---")
        self.db = Neo4jConnector(uri, user, password)
        
        self.llm_model = os.getenv("GRAPH_REASONING_MODEL", "qwen2.5-coder:7b-instruct")
        self.client = ollama.AsyncClient()
        self._embedding_cache: Dict[str, list] = {}
        self.graph_timeout_seconds = float(os.getenv("GRAPH_QUERY_TIMEOUT_SECONDS", "5"))
        self.graph_retry_count = int(os.getenv("GRAPH_QUERY_RETRY_COUNT", "1"))
        self.rewrite_model = os.getenv("REWRITE_MODEL", "qwen2.5-coder:1.5b-instruct")
        self.intent_model = os.getenv("INTENT_MODEL", "qwen2.5-coder:1.5b-instruct")
        self.cypher_model = os.getenv("CYPHER_MODEL", "qwen2.5-coder:3b-instruct")
        self.answer_primary_model = os.getenv("ANSWER_PRIMARY_MODEL", "qwen2.5-coder:7b-instruct")
        self.answer_fallback_model = os.getenv("ANSWER_FALLBACK_MODEL", "qwen2.5-coder:3b-instruct")
        self.max_context_chars = int(os.getenv("MAX_CONTEXT_CHARS", "2600"))
        self.max_graph_records = int(os.getenv("MAX_GRAPH_RECORDS", "10"))
        self.cache_ttl_seconds = int(os.getenv("CACHE_TTL_SECONDS", "900"))
        self.cache_max_size = int(os.getenv("CACHE_MAX_SIZE", "1000"))

        self._embedding_cache = TTLCache(ttl_seconds=self.cache_ttl_seconds, max_size=self.cache_max_size)
        self._rewrite_cache = TTLCache(ttl_seconds=self.cache_ttl_seconds, max_size=self.cache_max_size)
        self._cypher_cache = TTLCache(ttl_seconds=self.cache_ttl_seconds, max_size=self.cache_max_size)
        self._context_cache = TTLCache(ttl_seconds=self.cache_ttl_seconds, max_size=self.cache_max_size)

        self.metrics_counters: Dict[str, float] = {
            "requests": 0,
            "cypher_generated": 0,
            "template_cypher_hits": 0,
            "llm_cypher_hits": 0,
            "graph_hits": 0,
            "fallback_vector": 0,
            "fallback_none": 0,
            "total_rewrite_ms": 0.0,
            "total_graph_ms": 0.0,
        }

    def _normalize_text(self, text: Optional[str]) -> str:
        if not text:
            return ""
        text = text.strip().lower()
        text = unicodedata.normalize("NFD", text)
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        text = text.replace("đ", "d")
        text = re.sub(r"\s+", " ", text)
        return text

    async def _run_cypher(self, cypher: str, params: Dict[str, Any], single: bool = False):
        """Wrapper chạy truy vấn Neo4j có timeout và retry ngắn để ổn định khi tải cao."""
        last_error = None
        for attempt in range(self.graph_retry_count + 1):
            try:
                async with self.db.driver.session() as session:
                    result = await asyncio.wait_for(
                        session.run(cypher, **params),
                        timeout=self.graph_timeout_seconds,
                    )
                    if single:
                        record = await asyncio.wait_for(
                            result.single(),
                            timeout=self.graph_timeout_seconds,
                        )
                        return record

                    records = await asyncio.wait_for(
                        result.data(),
                        timeout=self.graph_timeout_seconds,
                    )
                    return records
            except Exception as e:
                last_error = e
                if attempt < self.graph_retry_count:
                    await asyncio.sleep(0.08 * (attempt + 1))

        raise last_error

    def extract_entities(self, query: str) -> Dict[str, Any]:
        """
        Extract structured entities from user query.
        Returns: {
            'major': str (ngành),
            'major_normalized': str,
            'loai_hinh': str (clc, đại trà, or None),
            'course_code': str,
            'course_name': str,
            'khoa_hoc': int (khóa học),
            'query_intent': str (credit, program, major_list, etc.)
        }
        """
        q = query.strip()
        q_norm = self._normalize_text(q)

        result = {
            "nganh": None,
            "attribute": None,
            "intent": "unknown",
            "major": None,
            "major_normalized": None,
            "loai_hinh": None,
            "course_code": None,
            "course_name": None,
            "khoa_hoc": None,
            "query_intent": None,
        }

        # 1. Extract course code (e.g., CT101, SE201)
        code_match = re.search(r"\b([A-Za-z]{2,3}\d{3}[A-Za-z]?)\b", q)
        if code_match:
            result["course_code"] = code_match.group(1).upper()

        # 2. Extract course name (after "môn học" or "môn")
        course_match = re.search(
            r"m[oô]n(?:\s+h[oọ]c)?\s+([^?.!,]+?)(?=\s+(?:c[oó]|l[aà]|bao|ti[eê]n|h[oọ]c)|[!?.]|$)",
            q,
            re.IGNORECASE,
        )
        if course_match:
            result["course_name"] = course_match.group(1).strip()

        # 3. Extract major name - up to keyword boundary
        major_match = re.search(
            r"ng[aà]nh\s+([^?.!,]+?)(?=\s+(?:c[oó]|kh[oô]ng|l[aà]|bao|m[aá]y|n[aà]o|clc|ch[aấ]t|đại|kh[oó]a|[!?.])|(?:c[oó]|[!?.])|$)",
            q,
            re.IGNORECASE,
        )
        if major_match:
            raw_major = major_match.group(1).strip()
            result["nganh"] = raw_major
            result["major"] = raw_major
            result["major_normalized"] = self._normalize_text(raw_major)

        # 4. Extract loại hình (CLC vs đại trà)
        if any(x in q_norm for x in ["clc", "chat luong cao", "chất lượng cao"]):
            result["loai_hinh"] = "clc"
        elif any(x in q_norm for x in ["dai tra", "đại trà"]):
            result["loai_hinh"] = "đại trà"

        # 4b. Extract requested attribute (strict normalized labels)
        if any(x in q_norm for x in ["tin chi", "tín chỉ"]):
            result["attribute"] = "tin_chi"
        elif any(x in q_norm for x in ["thoi gian", "thời gian", "bao lau", "mấy năm", "may nam"]):
            result["attribute"] = "thoi_gian"
        elif any(x in q_norm for x in ["muc tieu", "mục tiêu"]):
            result["attribute"] = "muc_tieu"
        elif any(x in q_norm for x in ["hoc phan", "học phần", "mon hoc", "môn học", "ma hoc phan", "mã học phần"]):
            result["attribute"] = "hoc_phan"

        # 5. Extract khóa học
        khoa_match = re.search(r"kh[oó]a\s*(\d{1,2})", q, re.IGNORECASE)
        if khoa_match:
            result["khoa_hoc"] = int(khoa_match.group(1))

        # 6. Infer query intent from question patterns
        if any(x in q_norm for x in ["bao nhieu tin chi", "so tin chi", "tong tin chi"]):
            result["query_intent"] = "ask_credit"
            result["intent"] = "ask_credit"
        elif any(x in q_norm for x in ["chuong trinh dao tao", "ctdt", "chương trình"]):
            result["query_intent"] = "ask_program"
            result["intent"] = "ask_program"
        elif any(x in q_norm for x in ["clc", "chat luong cao", "dai tra", "loai hinh"]):
            result["query_intent"] = "ask_program_type"
            result["intent"] = "ask_program_type"
        elif any(x in q_norm for x in ["co may nganh", "bao nhieu nganh", "nhung nganh"]):
            result["query_intent"] = "ask_major_list"
            result["intent"] = "ask_major_list"
        elif any(x in q_norm for x in ["ti[eê]n quy[eế]t"]):
            result["query_intent"] = "ask_prerequisite"
            result["intent"] = "ask_prerequisite"
        elif any(x in q_norm for x in ["thoi gian", "thời gian", "mấy năm", "may nam"]):
            result["query_intent"] = "ask_duration"
            result["intent"] = "ask_duration"
        else:
            result["query_intent"] = "ask_info"
            result["intent"] = "ask_info"

        if not result.get("nganh") and result.get("major"):
            result["nganh"] = result.get("major")

        return result

    
    async def _call_llm(self, prompt: str, system_prompt: str = "") -> str:
        """Sử dụng AsyncClient để gọi Ollama không chặn luồng"""
        try:
            response = await self.client.generate(
                model=self.llm_model,
                system=system_prompt,
                prompt=prompt,
                options=self._build_ollama_options(temperature=0.0)
            )
            return response['response'].strip()
        except Exception as e:
            return f"Lỗi gọi LLM: {str(e)}"

    def aggregator(self, graph_rows: list, query_intent: Optional[str]) -> Dict[str, Any]:
        """
        Process and aggregate graph results before building context.
        Handles deduplication, normalization, and computation.
        """
        if not graph_rows:
            return {
                "aggregated": [],
                "summary": None,
                "success": False,
            }

        # Special aggregation logic for credit queries
        if query_intent == "ask_credit":
            tong_tin_chi_values = []
            for row in graph_rows:
                if isinstance(row, dict):
                    ttc = row.get("tong_tin_chi")
                    if ttc is not None:
                        try:
                            tong_tin_chi_values.append(int(ttc))
                        except (ValueError, TypeError):
                            pass
            
            if tong_tin_chi_values:
                # For credit queries, return the first (most recent) value
                # Graph should be ordered by khoa DESC already
                return {
                    "aggregated": graph_rows,
                    "summary": {
                        "tong_tin_chi": tong_tin_chi_values[0],
                        "khoa_hoctin": graph_rows[0].get("khoa") if isinstance(graph_rows[0], dict) else None,
                    },
                    "success": True,
                }

        # For major list queries, deduplicate by ten_nganh_vi
        if query_intent == "ask_major_list":
            seen = {}
            deduplicated = []
            for row in graph_rows:
                if isinstance(row, dict):
                    nganh_vi = row.get("nganh")
                    if nganh_vi and nganh_vi not in seen:
                        seen[nganh_vi] = row
                        deduplicated.append(row)
                    elif nganh_vi:
                        # Merge if we have multiple loai_hinh for same nganh
                        pass
            
            return {
                "aggregated": deduplicated,
                "summary": {"count": len(deduplicated)},
                "success": len(deduplicated) > 0,
            }

        # For program type queries, keep duplicates but group by loai_hinh
        if query_intent == "ask_program_type":
            grouped = {}
            for row in graph_rows:
                if isinstance(row, dict):
                    lt = row.get("loai_hinh", "unknown")
                    if lt not in grouped:
                        grouped[lt] = []
                    grouped[lt].append(row)
            
            return {
                "aggregated": graph_rows,
                "summary": {"loai_hinh_types": list(grouped.keys())},
                "success": len(grouped) > 0,
            }

        # Default: return all rows as-is
        return {
            "aggregated": graph_rows,
            "summary": {"row_count": len(graph_rows)},
            "success": len(graph_rows) > 0,
        }

    def _get_query_embedding(self, query: str):
        cached = self._embedding_cache.get(query)
        if cached is not None:
            return cached
        vec = self.embed_service.get_embedding_batch([query])[0]
        self._embedding_cache.set(query, vec)
        return vec

    async def vector_search(self, query: str, top_k: int = 4, min_score: float = 0.7):
        query_vec = self._get_query_embedding(query)

        cypher = """
        CALL db.index.vector.queryNodes('global_knowledge_index', $k, $vector)
        YIELD node, score
        WHERE score >= $min_score
        RETURN labels(node)[0] as type, node.text as context, score
        ORDER BY score DESC LIMIT $k
        """

        try:
            records = await self._run_cypher(
                cypher,
                {"vector": query_vec, "k": top_k, "min_score": min_score},
                single=False,
            )
        except Exception as e:
            print(f"Lỗi vector search: {e}")
            return None

        unique = []
        seen = set()
        for row in records:
            text = (row.get("context") or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            unique.append(f"[{row.get('type', 'Entity')}|{row.get('score', 0):.3f}] {text}")

        return "\n".join(unique) if unique else None

    async def query_graph(self, intent: str, query: str, entities: Dict[str, Optional[str]]):
        code = entities.get("course_code")
        name = entities.get("course_name") or entities.get("major_name") or entities.get("search_term") or query
        name_norm = entities.get("search_term_norm") or self._normalize_text(name)
        major = entities.get("major_name") or name
        khoa_hoc = entities.get("khoa_hoc")

        try:
            if intent == "PREREQUISITE":
                cypher = """
                MATCH (h:HocPhan)
                WHERE ($code IS NOT NULL AND toUpper(h.ma_hp) = toUpper($code))
                   OR toLower(h.ten_hp) CONTAINS toLower($name)
                OPTIONAL MATCH (h)-[:YEU_CAU_TIEN_QUYET]->(pre:HocPhan)
                WITH h, collect(DISTINCT coalesce(pre.ma_hp + ' - ' + pre.ten_hp, pre.ten_hp)) as prerequisites,
                     CASE
                        WHEN $code IS NOT NULL AND toUpper(h.ma_hp) = toUpper($code) THEN 3
                        WHEN toLower(h.ten_hp) = toLower($name) THEN 2
                        ELSE 1
                     END as rank
                RETURN h.ma_hp as ma_hp, h.ten_hp as ten_hp, prerequisites
                ORDER BY rank DESC
                LIMIT 3
                """
                rows = await self._run_cypher(cypher, {"code": code, "name": name}, single=False)
                if rows:
                    items = []
                    for row in rows:
                        pres = [x for x in row.get("prerequisites", []) if x]
                        if pres:
                            items.append(
                                f"Môn {row.get('ma_hp', '')} {row.get('ten_hp', '')} cần học trước: {', '.join(pres)}"
                            )
                    if items:
                        return "\n".join(items)

            elif intent in ["CREDIT", "COURSE_INFO"]:
                cypher = """
                MATCH (h:HocPhan)
                WHERE ($code IS NOT NULL AND toUpper(h.ma_hp) = toUpper($code))
                   OR toLower(h.ten_hp) CONTAINS toLower($name)
                OPTIONAL MATCH (h)-[:YEU_CAU_TIEN_QUYET]->(pre:HocPhan)
                WITH h, collect(DISTINCT pre.ma_hp) as pre_codes,
                     CASE
                        WHEN $code IS NOT NULL AND toUpper(h.ma_hp) = toUpper($code) THEN 3
                        WHEN toLower(h.ten_hp) = toLower($name) THEN 2
                        ELSE 1
                     END as rank
                RETURN h.ma_hp as ma_hp, h.ten_hp as ten_hp, h.so_tin_chi as credits,
                       h.so_tiet_ly_thuyet as ly_thuyet, h.so_tiet_thuc_hanh as thuc_hanh,
                       h.bat_buoc as bat_buoc, pre_codes
                ORDER BY rank DESC
                LIMIT 5
                """
                rows = await self._run_cypher(cypher, {"code": code, "name": name}, single=False)
                if rows:
                    lines = []
                    for r in rows:
                        base = f"Môn {r['ma_hp']} - {r['ten_hp']} có {r['credits']} tín chỉ"
                        if intent == "COURSE_INFO":
                            base += (
                                f", LT/TH: {r.get('ly_thuyet', 0)}/{r.get('thuc_hanh', 0)}, "
                                f"{'bắt buộc' if r.get('bat_buoc') else 'tự chọn'}"
                            )
                            if r.get("pre_codes"):
                                base += f", tiên quyết: {', '.join([x for x in r['pre_codes'] if x])}"
                        lines.append(base)
                    return "\n".join(lines)

            elif intent in ["PROGRAM", "DURATION", "DEGREE", "TRAINING_FORM", "FACULTY", "MAJOR_INFO"]:
                cypher = """
                MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
                WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($major)
                   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
                   OR toLower(coalesce(n.ten_nganh_vi, '') + ' ' + coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
                OPTIONAL MATCH (n)-[:THUOC_VE]->(dv)
                OPTIONAL MATCH (ctdt)-[:CO_LOAI_VAN_BANG]->(degree:LoaiVanBang)
                OPTIONAL MATCH (ctdt)-[:DAO_TAO_THEO_HINH_THUC]->(form:HinhThucDaoTao)
                OPTIONAL MATCH (ctdt)-[:DAO_TAO_THEO_PHUONG_THUC]->(method:PhuongThucDaoTao)
                WITH ctdt, n,
                     collect(DISTINCT coalesce(properties(dv)['ten_khoa'], properties(dv)['ten_bomon'], properties(dv)['ten'])) as don_vi,
                     collect(DISTINCT degree.loai_van_bang) as van_bang,
                     collect(DISTINCT form.ten_hinh_thuc) as hinh_thuc,
                     collect(DISTINCT method.ten_phuong_thuc) as phuong_thuc,
                     CASE
                        WHEN toLower(coalesce(n.ten_nganh_vi, '')) = toLower($major) THEN 3
                        WHEN toLower(coalesce(n.ten_nganh_en, '')) = toLower($major) THEN 2
                        ELSE 1
                     END as rank
                WHERE $khoa_hoc IS NULL OR toString(ctdt.khoa) = $khoa_hoc
                RETURN n.ten_nganh_vi as major_vi,
                       n.ten_nganh_en as major_en,
                       ctdt.khoa as khoa_hoc,
                       ctdt.tong_tin_chi as tong_tin_chi,
                      coalesce(properties(ctdt)['thoi_gian_dao_tao'], properties(ctdt)['thoi_gian']) as thoi_gian,
                       ctdt.loai_hinh as loai_hinh,
                       ctdt.ngon_ngu as ngon_ngu,
                       don_vi, van_bang, hinh_thuc, phuong_thuc,
                       rank
                ORDER BY rank DESC, ctdt.khoa DESC
                LIMIT 3
                """
                rows = await self._run_cypher(cypher, {"major": major, "khoa_hoc": khoa_hoc}, single=False)
                if rows:
                    lines = []
                    for row in rows:
                        lines.append(
                            (
                                f"Ngành {row.get('major_vi', '')} ({row.get('major_en', '')}) khóa {row.get('khoa_hoc')}: "
                                f"{row.get('tong_tin_chi')} tín chỉ, {row.get('thoi_gian')} năm, "
                                f"loại hình {row.get('loai_hinh')}, ngôn ngữ {row.get('ngon_ngu')}. "
                                f"Đơn vị: {', '.join([x for x in row.get('don_vi', []) if x])}. "
                                f"Văn bằng: {', '.join([x for x in row.get('van_bang', []) if x])}. "
                                f"Hình thức: {', '.join([x for x in row.get('hinh_thuc', []) if x])}. "
                                f"Phương thức: {', '.join([x for x in row.get('phuong_thuc', []) if x])}."
                            )
                        )
                    return "\n".join(lines)

            elif intent == "CAREER":
                cypher = """
                MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
                WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($major)
                   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
                MATCH (ctdt)-[:CO_CO_HOI_VIEC_LAM]->(job:ViTriViecLam)
                RETURN DISTINCT job.noi_dung as career
                LIMIT 8
                """
                rows = await self._run_cypher(cypher, {"major": major}, single=False)
                if rows:
                    return "\n".join([f"- {r['career']}" for r in rows if r.get("career")])

            elif intent == "OUTCOME":
                cypher = """
                MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
                WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($major)
                   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
                MATCH (ctdt)-[:DAT_CHUAN_DAU_RA]->(cdr:ChuanDauRa)
                RETURN cdr.noi_dung as content, cdr.loai as loai, cdr.nhom as nhom
                LIMIT 10
                """
                rows = await self._run_cypher(cypher, {"major": major}, single=False)
                if rows:
                    return "\n".join(
                        [f"[{r.get('loai', 'Chung')} - {r.get('nhom', 'Chung')}] {r.get('content', '')}" for r in rows]
                    )

            elif intent == "FURTHER_STUDY":
                cypher = """
                MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
                WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($major)
                   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
                MATCH (ctdt)-[:TAO_NEN_TANG]->(k:KhaNangHocTap)
                RETURN DISTINCT k.noi_dung as content
                LIMIT 8
                """
                rows = await self._run_cypher(cypher, {"major": major}, single=False)
                if rows:
                    return "\n".join([f"- {r['content']}" for r in rows if r.get("content")])

        except Exception as e:
            print(f"Lỗi Graph: {e}")

        return None
    
    def build_context(self, intent: str, entities: Dict[str, Optional[str]], graph_data: Optional[str], vector_data: Optional[str]):
        context_parts = [
            f"[INTENT]\n{intent}",
            f"[ENTITIES]\n{json.dumps(entities, ensure_ascii=False)}",
        ]

        if graph_data:
            context_parts.append(f"[GRAPH]\n{graph_data}")

        if vector_data:
            context_parts.append(f"[VECTOR]\n{vector_data}")

        return "\n\n".join(context_parts).strip()

    # -----------------------
    # Modular pipeline blocks
    # -----------------------
    async def query_rewrite(self, query: str, history: Optional[list] = None) -> str:
        history = history or []
        raw = query.strip()
        if len(raw) < 3:
            return raw

        # Rule-based coreference resolution cho câu follow-up phổ biến để tránh rewrite sai nghĩa.
        raw_norm_early = self._normalize_text(raw)

        # Xử lý xác nhận ngắn theo ngữ cảnh hội thoại gần nhất.
        if raw_norm_early in ["co", "có", "ok", "duoc", "được", "yes", "uh", "uhm", "um"]:
            last_assistant = ""
            for msg in reversed(history[-5:]):
                if msg.get("role") == "assistant":
                    last_assistant = self._normalize_text(msg.get("content", ""))
                    break
            if any(x in last_assistant for x in ["liet ke cac nganh dang co clc", "liệt kê các ngành đang có clc", "chuong trinh chat luong cao"]):
                return "Liệt kê các ngành đang có chương trình chất lượng cao (CLC) tại CTU"

        if any(x in raw_norm_early for x in [
            "do la nhung nganh gi",
            "do la nganh gi",
            "do la nhung nganh nao",
            "do la nganh nao",
            "nhung nganh gi",
            "nhung nganh nao",
            "liet ke cac nganh",
            "liệt kê các ngành",
            "danh sach nganh",
            "danh sách ngành",
        ]):
            history_text = self._normalize_text(" ".join([h.get("content", "") for h in history[-5:]]))
            if any(x in history_text for x in ["ctu co may nganh", "ctu co mấy ngành", "co 9 nganh", "so_nganh"]):
                return "Liệt kê các ngành đào tạo của CTU"

        # Rewrite ngắn mang tính xác nhận nhưng không rõ ngữ nghĩa.
        if raw_norm_early in ["co", "có"]:
            return "Người dùng xác nhận yêu cầu trước đó"

        # Fast-path: câu đã rõ nghĩa, không có đồng tham chiếu => giữ nguyên để giảm latency.
        raw_norm = self._normalize_text(raw)
        has_coref = any(x in raw_norm.split() for x in ["no", "cai", "do", "nay", "kia", "ấy", "đó"])
        has_question_signal = any(x in raw_norm for x in ["bao nhieu", "mấy", "khong", "la gi", "tra cuu", "tim", "xem", "?", "tin chi", "nganh nao", "nganh gi"])
        if not has_coref and has_question_signal:
            return raw

        cache_key = f"rewrite::{self._normalize_text(raw)}::{self._normalize_text(' '.join([h.get('content', '') for h in history[-5:]]))}"
        cached = self._rewrite_cache.get(cache_key)
        if cached is not None:
            return cached

        prompt = (
            "STRICT CONTROL PROMPT FOR GRAPHRAG PIPELINE\n"
            "Bạn là một thành phần deterministic.\n"
            "GLOBAL RULES:\n"
            "- KHÔNG trả lời người dùng.\n"
            "- KHÔNG giải thích.\n"
            "- KHÔNG hallucinate dữ liệu.\n"
            "- Chỉ được rewrite câu hỏi.\n\n"
            "TASK 1: QUERY REWRITE\n"
            "- Giữ nguyên ý nghĩa gốc tuyệt đối.\n"
            "- Không paraphrase khi câu đã rõ.\n"
            "- Nếu mơ hồ (nó, có, vậy, cái đó) thì mở rộng từ lịch sử.\n"
            "- Output duy nhất là câu query đã rewrite, không markdown.\n"
            "- Nếu không chắc, trả lại câu gốc.\n"
        )
        messages = history[-5:] + [{"role": "user", "content": raw}]
        try:
            resp = await self.client.chat(
                model=self.rewrite_model,
                messages=[{"role": "system", "content": prompt}] + messages,
            )
            rewritten = (resp.get("message", {}).get("content") or "").strip()
            if not rewritten:
                rewritten = raw
        except Exception:
            rewritten = raw

        # Guard: nếu rewrite có dấu hiệu trả lời/đưa facts mới thì bỏ, dùng query gốc.
        rw_norm = self._normalize_text(rewritten)
        suspicious_prefixes = ["ban can", "ctu co", "la", "bao gom", "co hon", "duoc", "hay", "vui long", "vui lòng", "tra loi", "cau tra loi"]
        if any(rw_norm.startswith(p) for p in suspicious_prefixes):
            rewritten = raw

        nums_raw = re.findall(r"\d+", raw)
        nums_rw = re.findall(r"\d+", rewritten)
        if nums_rw and nums_rw != nums_raw:
            rewritten = raw

        # Rewrite phải là câu hỏi hoặc mệnh lệnh tra cứu ngắn gọn, không phải câu trả lời.
        query_like_patterns = [
            r"\bbao nhieu\b",
            r"\btra cuu\b",
            r"\btim\b",
            r"\bxem\b",
            r"\bla gi\b",
            r"\bnganh nao\b",
            r"\bnganh gi\b",
        ]
        looks_like_query = ("?" in rewritten) or any(re.search(p, rw_norm) for p in query_like_patterns)
        if not looks_like_query:
            rewritten = raw

        self._rewrite_cache.set(cache_key, rewritten)
        return rewritten

    async def intent_classifier(self, query: str) -> str:
        """Improved intent classification with explicit rules and better boundaries."""
        q = query.lower().strip()
        q_norm = self._normalize_text(query)

        # 1. Instruction/control commands (language switching)
        if any(x in q_norm for x in ["tra loi bang tieng viet", "trả lời bằng tiếng việt", "noi tieng viet", "nói tiếng việt"]):
            return "chitchat"

        # 2. Pure greetings - must be EXACT match or very short
        if re.fullmatch(r"(xin\s*chao|chao|hello|hi|hey)(\s+ban)?[!.,?]*", q_norm):
            return "chitchat"
        if re.fullmatch(r"(cam\s*on|cảm\s*ơn|thanks|thank\s+you)(\s+ban)?[!.,?]*", q_norm):
            return "chitchat"
        
        # 3. Very short unclear queries
        if len(q) <= 3:
            return "chitchat"

        # 4. Confirmation/short replies in context
        if q_norm in ["co", "có", "ok", "duoc", "được", "yes", "uh", "uhm", "um", "vang", "vâng"]:
            return "chitchat"

        # 5. Explanation/comparison queries
        if any(x in q_norm for x in ["vi sao", "tai sao", "why", "khac nhau", "so sanh", "giai thich", "difference"]):
            return "explanation"

        # 6. Rule/policy queries
        if any(x in q_norm for x in ["quy che", "quy dinh", "dieu khoan", "chính sách", "policy", "regulation"]):
            return "factual_graph"

        # 7. Credit/program queries - STRONGEST signals
        if any(x in q_norm for x in ["bao nhieu tin chi", "so tin chi", "tong tin chi"]):
            return "factual_graph"
        if any(x in q_norm for x in ["chuong trinh dao tao", "ctdt"]):
            return "factual_graph"
        if any(x in q_norm for x in ["clc", "chat luong cao", "dai tra", "loai hinh"]):
            return "factual_graph"

        # 8. Major/program info queries
        if any(x in q_norm for x in ["nganh", "chuyên ngành", "major", "department"]):
            return "factual_graph"
        if any(x in q_norm for x in ["co may nganh", "bao nhieu nganh", "nhung nganh", "danh sach nganh"]):
            return "factual_graph"

        # 9. Course/learning queries
        if any(x in q_norm for x in ["mon hoc", "hoc phan", "course", "tien quyet"]):
            return "factual_graph"

        # 10. Data lookup keywords
        if any(x in q_norm for x in ["tra cuu", "tim", "xem", "khong", "bao"]):
            return "factual_graph"

        # Fallback to model-based classification
        prompt = (
            "Classify the following question into ONE of these categories:\n"
            "- factual_graph: asking for specific data (programs, majors, credits, rules)\n"
            "- explanation: asking for comparison or explanation\n"
            "- chitchat: greeting or casual chat\n\n"
            "Return ONLY the category name, nothing else."
        )
        try:
            resp = await self.client.chat(
                model=self.intent_model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": query},
                ],
            )
            label = (resp.get("message", {}).get("content") or "").strip().lower()
            for word in ["explanation", "chitchat", "factual_graph"]:
                if word in label:
                    return word
            return "factual_graph"
        except Exception:
            return "factual_graph"

    def _graph_schema_prompt(self) -> str:
        return (
            "Nodes chính: ChuongTrinhDaoTao, Nganh, HocPhan, KhoiKienThuc, YeuCauTuChon, "
            "NhomHocPhanTuChon, ChuanDauRa, VanBanPhapLy, ViTriViecLam, KhaNangHocTap, TrinhDo, LoaiVanBang, HinhThucDaoTao, PhuongThucDaoTao.\n"
            "Quan hệ chính: THUOC_VE, DAO_TAO_TRINH_DO, CO_LOAI_VAN_BANG, DAO_TAO_THEO_HINH_THUC, DAO_TAO_THEO_PHUONG_THUC, "
            "CO_MUC_TIEU_DAO_TAO, CO_CO_HOI_VIEC_LAM, TUAN_THU, THAM_CHIEU, TAO_NEN_TANG, DAT_CHUAN_DAU_RA, CO_KHOI_KIEN_THUC, "
            "CO_YEU_CAU_TU_CHON, CO_NHOM_THANH_PHAN, GOM_HOC_PHAN, YEU_CAU_TIEN_QUYET, CO_THE_SONG_HANH.\n"
            "Thuộc tính quan trọng: HocPhan.ma_hp/ten_hp/so_tin_chi, Nganh.ten_nganh_vi/ten_nganh_en, ChuongTrinhDaoTao.khoa/tong_tin_chi/thoi_gian_dao_tao."
        )

    def template_cypher_from_query(self, query: str, entities: Dict[str, Any]) -> Optional[str]:
        """
        Generate Cypher queries using extracted entities, NOT full user query.
        Returns: (cypher_string, params_dict) or None
        """
        major = entities.get("nganh") or entities.get("major") or entities.get("major_normalized") or ""
        major_norm = entities.get("major_normalized") or self._normalize_text(major)
        loai_hinh = entities.get("loai_hinh")
        khoa_hoc = entities.get("khoa_hoc")
        query_intent = entities.get("query_intent") or entities.get("intent")
        code = entities.get("course_code")

        # Remove accents for database matching
        major_for_query = major_norm.replace(" ", "").lower() if major_norm else ""

        # Query 1: List programs with specific loai_hinh (CLC)
        if query_intent == "ask_program_type" and loai_hinh == "clc":
            return """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(ctdt.loai_hinh, '')) = toLower($loai_hinh)
RETURN DISTINCT n.ten_nganh_vi as nganh, n.ten_nganh_en as nganh_en, ctdt.loai_hinh as loai_hinh
ORDER BY nganh LIMIT 20
""".strip()

        # Query 2: Ask credit for specific major + optional loai_hinh
        elif query_intent == "ask_credit" and major:
            cypher = """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($major)
   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
"""
            if loai_hinh:
                cypher += "\nAND toLower(coalesce(ctdt.loai_hinh, '')) = toLower($loai_hinh)"
            
            cypher += """
RETURN n.ten_nganh_vi as nganh, ctdt.khoa as khoa, ctdt.tong_tin_chi as tong_tin_chi
ORDER BY ctdt.khoa DESC LIMIT 3
"""
            return cypher.strip()

        # Query 3: List all programs for major with loai_hinh info
        elif query_intent == "ask_program" and major:
            cypher = """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($major)
   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($major)
"""
            if khoa_hoc:
                cypher += f"\nAND ctdt.khoa = {khoa_hoc}"
            
            cypher += """
RETURN n.ten_nganh_vi as nganh,
       collect(DISTINCT ctdt.loai_hinh) as cac_loai_hinh,
       collect(DISTINCT ctdt.khoa) as cac_khoa
LIMIT 5
"""
            return cypher.strip()

        # Query 4: List all majors
        elif query_intent == "ask_major_list":
            return """
MATCH (n:Nganh)
RETURN DISTINCT n.ten_nganh_vi as nganh, n.ten_nganh_en as nganh_en
ORDER BY nganh LIMIT 20
""".strip()

        # Query 5: Count majors
        elif query_intent == "ask_major_list":
            return """
MATCH (n:Nganh)
RETURN count(DISTINCT n) as so_nganh LIMIT 1
""".strip()

        # Query 6: Course code lookup
        elif code:
            return f"""
MATCH (h:HocPhan)
WHERE toUpper(h.ma_hp) = toUpper($code)
OPTIONAL MATCH (h)-[:YEU_CAU_TIEN_QUYET]->(pre:HocPhan)
RETURN h.ma_hp as ma_hp, h.ten_hp as ten_hp, h.so_tin_chi as tin_chi,
       collect(DISTINCT pre.ma_hp) as prerequisites
LIMIT 3
""".strip()

        return None

    def _enforce_cypher_limit(self, cypher: str, max_records: int = 10) -> str:
        clean = cypher.strip().rstrip(";")
        if re.search(r"\bLIMIT\b", clean, flags=re.IGNORECASE):
            clean = re.sub(r"\bLIMIT\s+\d+", f"LIMIT {max_records}", clean, flags=re.IGNORECASE)
            return clean
        return f"{clean}\nLIMIT {max_records}"

    def _is_safe_cypher(self, cypher: str) -> bool:
        clean = (cypher or "").strip()
        if not clean:
            return False

        upper = clean.upper()
        blocked_tokens = [
            "CREATE ",
            "MERGE ",
            "DELETE ",
            "DETACH DELETE",
            "SET ",
            "REMOVE ",
            "DROP ",
            "LOAD CSV",
            "CALL DBMS",
            "APOC.",
            "FOREACH",
            "UNWIND",
        ]
        if any(tok in upper for tok in blocked_tokens):
            return False

        # Chỉ cho phép luồng read-only chuẩn bắt đầu bằng MATCH hoặc OPTIONAL MATCH
        stripped = upper.lstrip()
        if not (stripped.startswith("MATCH") or stripped.startswith("OPTIONAL MATCH")):
            return False

        # Bắt buộc có RETURN để trả dữ liệu đọc (dùng regex để không phụ thuộc khoảng trắng/newline)
        if not re.search(r"\bRETURN\b", upper):
            return False

        return True

    def _sanitize_cypher(self, cypher: Optional[str], max_records: int) -> Optional[str]:
        if not cypher:
            return None
        limited = self._enforce_cypher_limit(cypher, max_records=max_records)
        if not self._is_safe_cypher(limited):
            return None
        return limited

    def _new_metrics(self) -> Dict[str, Any]:
        return {
            "rewrite_ms": 0.0,
            "graph_ms": 0.0,
            "cypher_generated": False,
            "cypher_source": "none",
            "graph_hit": False,
            "graph_rows_count": 0,
            "fallback": "none",
            "cache_sizes": {
                "rewrite": self._rewrite_cache.size(),
                "cypher": self._cypher_cache.size(),
                "context": self._context_cache.size(),
                "embedding": self._embedding_cache.size(),
            },
        }

    def _update_metrics_counters(self, metrics: Dict[str, Any]):
        self.metrics_counters["requests"] += 1
        self.metrics_counters["total_rewrite_ms"] += float(metrics.get("rewrite_ms", 0.0))
        self.metrics_counters["total_graph_ms"] += float(metrics.get("graph_ms", 0.0))
        if metrics.get("cypher_generated"):
            self.metrics_counters["cypher_generated"] += 1
        if metrics.get("cypher_source") == "template":
            self.metrics_counters["template_cypher_hits"] += 1
        if metrics.get("cypher_source") == "llm":
            self.metrics_counters["llm_cypher_hits"] += 1
        if metrics.get("graph_hit"):
            self.metrics_counters["graph_hits"] += 1

        fallback = metrics.get("fallback")
        if fallback == "vector":
            self.metrics_counters["fallback_vector"] += 1
        elif fallback == "none":
            self.metrics_counters["fallback_none"] += 1

    def get_metrics_snapshot(self) -> Dict[str, Any]:
        req = max(int(self.metrics_counters["requests"]), 1)
        cypher_total = max(int(self.metrics_counters["cypher_generated"]), 1)
        return {
            "requests": int(self.metrics_counters["requests"]),
            "cypher_hit_rate": float(self.metrics_counters["cypher_generated"]) / req,
            "template_hit_rate": float(self.metrics_counters["template_cypher_hits"]) / req,
            "llm_cypher_hit_rate": float(self.metrics_counters["llm_cypher_hits"]) / req,
            "template_share_in_cypher": float(self.metrics_counters["template_cypher_hits"]) / cypher_total,
            "llm_share_in_cypher": float(self.metrics_counters["llm_cypher_hits"]) / cypher_total,
            "graph_hit_rate": float(self.metrics_counters["graph_hits"]) / req,
            "fallback_rate": float(self.metrics_counters["fallback_vector"]) / req,
            "avg_rewrite_ms": float(self.metrics_counters["total_rewrite_ms"]) / req,
            "avg_graph_ms": float(self.metrics_counters["total_graph_ms"]) / req,
            "cache_sizes": {
                "rewrite": self._rewrite_cache.size(),
                "cypher": self._cypher_cache.size(),
                "context": self._context_cache.size(),
                "embedding": self._embedding_cache.size(),
            },
        }

    async def cypher_generator(self, rewritten_query: str, intent_type: str, history: Optional[list] = None) -> Optional[str]:
        if intent_type not in ["factual_graph", "explanation"]:
            return None

        cache_key = f"cypher::{self._normalize_text(rewritten_query)}::{intent_type}"
        cached = self._cypher_cache.get(cache_key)
        if cached is not None:
            return cached

        entities = self.extract_entities(rewritten_query)
        strict_payload = {
            "nganh": entities.get("nganh"),
            "loai_hinh": entities.get("loai_hinh"),
            "attribute": entities.get("attribute"),
            "intent": entities.get("intent"),
        }

        prompt = (
            "STRICT CONTROL PROMPT FOR GRAPHRAG PIPELINE\n"
            "GLOBAL RULES:\n"
            "- KHÔNG trả lời user, KHÔNG giải thích.\n"
            "- KHÔNG tạo schema/quan hệ/thuộc tính mới.\n"
            "- Chỉ dùng schema được cung cấp.\n"
            "- Nếu không chắc hoặc không hỗ trợ bởi schema, trả: null\n\n"
            "TASK 3: CYPHER GENERATION\n"
            f"Schema:\n{self._graph_schema_prompt()}\n\n"
            "Input entities JSON:\n"
            f"{json.dumps(strict_payload, ensure_ascii=False)}\n\n"
            "Rules:\n"
            "1. Chỉ output duy nhất câu Cypher hoặc null.\n"
            "2. Tuyệt đối không dùng toàn bộ user query trong CONTAINS.\n"
            "3. Chỉ dùng entity đã extract.\n"
            "4. Cypher phải read-only, có LIMIT <= 10.\n"
            "5. Nếu entity invalid/schema mismatch -> null."
        )

        try:
            resp = await self.client.chat(
                model=self.cypher_model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": rewritten_query},
                ],
            )
            text = (resp.get("message", {}).get("content") or "").strip()
            if text.lower() == "null":
                return None
            match = re.search(r"```cypher(.*?)```", text, re.DOTALL | re.IGNORECASE)
            cypher = match.group(1).strip() if match else text
            if not cypher or "MATCH" not in cypher.upper():
                return None
            safe_cypher = self._sanitize_cypher(cypher, max_records=self.max_graph_records)
            if not safe_cypher:
                return None
            self._cypher_cache.set(cache_key, safe_cypher)
            return safe_cypher
        except Exception as e:
            print(f"Lỗi cypher_generator: {e}")
            return None

    async def graph_retriever(self, cypher: Optional[str], params: Optional[Dict[str, Any]] = None, max_records: int = 10) -> list:
        """Execute Cypher with parameters and return results."""
        if not cypher:
            return []
        try:
            safe_cypher = self._sanitize_cypher(cypher, max_records=max_records)
            if not safe_cypher:
                return []
            params = params or {}
            rows = await self._run_cypher(safe_cypher, params, single=False)
            return rows[:max_records] if rows else []
        except Exception as e:
            print(f"Lỗi graph_retriever: {e}")
            return []

    def context_builder(self, graph_rows: list, vector_data: Optional[str], entities: Optional[Dict[str, Any]] = None) -> str:
        """Build structured context from graph results and vectors."""
        if not graph_rows and not vector_data:
            return ""

        sections = []

        # Add structured entity info if available
        if entities:
            query_intent = entities.get("query_intent")
            major = entities.get("major")
            loai_hinh = entities.get("loai_hinh")
            entity_parts = []
            
            if major:
                entity_parts.append(f"Ngành: {major}")
            if loai_hinh:
                entity_parts.append(f"Loại hình: {loai_hinh}")
            if query_intent:
                entity_parts.append(f"Ý định: {query_intent}")
            
            if entity_parts:
                sections.append(f"[Thông tin truy vấn]\n- " + "\n- ".join(entity_parts))

        # Process graph results into sections
        if graph_rows:
            graph_section = []
            for row in graph_rows[:self.max_graph_records]:
                if not isinstance(row, dict):
                    continue
                line_parts = []
                for key, value in row.items():
                    if value is None or value == "":
                        continue
                    if isinstance(value, list):
                        value_str = ", ".join([str(v) for v in value if v is not None])
                    else:
                        value_str = str(value)
                    
                    if value_str.strip():
                        line_parts.append(f"{key}: {value_str}")
                
                if line_parts:
                    line = "- " + "; ".join(line_parts)
                    graph_section.append(line)
            
            if graph_section:
                sections.append("[Thông tin chương trình]\n" + "\n".join(graph_section[:10]))

        # Add cleaned vector data
        if vector_data:
            vector_lines = []
            for line in vector_data.split("\n"):
                t = line.strip()
                if not t:
                    continue
                # Filter out markdown table noise
                if "|" in t and t.count("|") >= 4:
                    continue
                # Clean up whitespace
                t = re.sub(r"\s+", " ", t)
                # Truncate long lines
                if len(t) > 260:
                    t = t[:257] + "..."
                vector_lines.append(f"- {t}")
            
            if vector_lines:
                sections.append("[Quy chế]\n" + "\n".join(vector_lines[:5]))

        context = "\n\n".join(sections).strip()
        return context[:self.max_context_chars]

    def _needs_numeric_answer(self, query: str) -> bool:
        q = self._normalize_text(query)
        return any(x in q for x in ["bao nhieu", "so tin chi", "muc", "toi thieu", "mấy", "muc gpa", "gpa", "nam hoc"])

    def build_no_data_reply(self, query: str, context: str = "") -> str:
        q = self._normalize_text(query)
        c = self._normalize_text(context)

        if any(x in q for x in ["clc", "chat luong cao"]):
            if "dai tra" in c:
                return (
                    "Mình chưa thấy dữ liệu về chương trình Chất lượng cao (CLC) cho ngành bạn hỏi, "
                    "nhưng hiện có dữ liệu chương trình Đại trà. Bạn muốn mình liệt kê các ngành đang có CLC để bạn tham khảo không?"
                )
            return (
                "Mình chưa tìm thấy dữ liệu chương trình Chất lượng cao (CLC) cho ngành này trong hệ thống hiện tại. "
                "Bạn có thể cho mình tên ngành đầy đủ để mình kiểm tra lại kỹ hơn nhé."
            )

        return "Mình chưa tìm thấy thông tin phù hợp trong dữ liệu hiện có. Bạn thử nêu rõ tên ngành hoặc mã học phần để mình tra chính xác hơn nhé."

    async def answer_generator(self, query: str, context: str, history: Optional[list] = None) -> str:
        if not context:
            return self.build_no_data_reply(query, context)

        history = history or []
        system_prompt = (
            "Bạn là trợ lý tư vấn CTU thân thiện.\n"
            "Chỉ được trả lời dựa trên CONTEXT được cung cấp.\n"
            "Nếu CONTEXT không đủ để kết luận, trả về đúng chuỗi: Không tìm thấy thông tin\n"
            "Không suy đoán, không thêm thông tin ngoài CONTEXT.\n"
            "Trả lời tự nhiên, dễ hiểu, ưu tiên 2-4 câu.\n"
            "Nếu là câu liệt kê, trình bày dạng gạch đầu dòng ngắn gọn."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"CONTEXT:\n{context}\n\nCâu hỏi: {query}"},
        ]

        models = [self.answer_primary_model]
        if self.answer_fallback_model != self.answer_primary_model:
            models.append(self.answer_fallback_model)

        last_error = None
        for model_name in models:
            try:
                resp = await self.client.chat(model=model_name, messages=messages)
                answer = (resp.get("message", {}).get("content") or "").strip()
                if not answer:
                    continue
                if self._needs_numeric_answer(query) and not re.search(r"\d", answer):
                    return self.build_no_data_reply(query, context)
                if self._normalize_text(answer) == self._normalize_text("Không tìm thấy thông tin"):
                    return self.build_no_data_reply(query, context)
                return answer
            except Exception as e:
                last_error = e

        if last_error:
            print(f"Lỗi answer_generator: {last_error}")
        return self.build_no_data_reply(query, context)

    async def run_main_pipeline(self, query: str, history: Optional[list] = None) -> Dict[str, Any]:
        history = history or []
        query = (query or "").strip()
        metrics = self._new_metrics()
        if len(query) < 2:
            self._update_metrics_counters(metrics)
            return {
                "rewritten_query": query,
                "intent_type": "chitchat",
                "cypher": None,
                "graph_rows": [],
                "vector_data": None,
                "context": "",
                "metrics": metrics,
            }

        # Stage 1: Rewrite
        rewrite_start = time.perf_counter()
        rewritten_query = await self.query_rewrite(query, history=history[-5:])
        metrics["rewrite_ms"] = round((time.perf_counter() - rewrite_start) * 1000, 2)

        # Stage 2: Intent & Entity extraction
        intent_type = await self.intent_classifier(rewritten_query)
        entities = self.extract_entities(rewritten_query)
        query_intent = entities.get("query_intent")

        cypher = None
        graph_rows = []
        vector_data = None

        if intent_type in ["factual_graph", "explanation"]:
            graph_start = time.perf_counter()
            
            # Stage 3: Cypher Generation - template first
            template_cypher = self.template_cypher_from_query(rewritten_query, entities)
            if not template_cypher:
                # Fallback: try template on original query if rewrite broke it
                template_cypher = self.template_cypher_from_query(query, self.extract_entities(query))
            
            if template_cypher:
                cypher = template_cypher
                metrics["cypher_source"] = "template"
            else:
                # LLM-based cypher generation
                cypher = await self.cypher_generator(rewritten_query, intent_type=intent_type, history=history[-5:])
                if cypher:
                    metrics["cypher_source"] = "llm"
            
            metrics["cypher_generated"] = bool(cypher)
            
            # Stage 4: Graph Retrieval
            cypher_params = {
                "major": entities.get("nganh") or entities.get("major"),
                "loai_hinh": entities.get("loai_hinh"),
                "code": entities.get("course_code"),
            }
            graph_rows = await self.graph_retriever(cypher, params=cypher_params, max_records=self.max_graph_records)

            # Stage 5: Aggregation - process results before context
            aggregation_result = self.aggregator(graph_rows, query_intent)
            aggregated_rows = aggregation_result.get("aggregated", [])

            metrics["graph_ms"] = round((time.perf_counter() - graph_start) * 1000, 2)
            metrics["graph_hit"] = bool(aggregated_rows)
            metrics["graph_rows_count"] = len(aggregated_rows)

            # STRICT FALLBACK CONTROL:
            # Only fallback to vector for long-text queries (rules, descriptions)
            # Never fallback for simple factual queries (major list, credit, program type)
            should_avoid_vector_fallback = query_intent in [
                "ask_major_list",
                "ask_credit",
                "ask_program_type",
            ]

            if not aggregated_rows and not should_avoid_vector_fallback:
                vector_data = await self.vector_search(rewritten_query, top_k=4, min_score=0.68)
                metrics["fallback"] = "vector" if vector_data else "none"
                # Use original graph_rows for context if no vector fallback
                if not vector_data:
                    aggregated_rows = graph_rows
            else:
                metrics["fallback"] = "none"
                aggregated_rows = graph_rows

        # Stage 6: Context Building
        context = self.context_builder(graph_rows=aggregated_rows, vector_data=vector_data, entities=entities)

        cache_key = f"ctx::{self._normalize_text(rewritten_query)}::{intent_type}"
        self._context_cache.set(cache_key, context)

        self._update_metrics_counters(metrics)

        return {
            "rewritten_query": rewritten_query,
            "intent_type": intent_type,
            "cypher": cypher,
            "graph_rows": aggregated_rows,
            "vector_data": vector_data,
            "context": context,
            "metrics": metrics,
            "metrics_summary": self.get_metrics_snapshot(),
        }

    # TẦNG 3: DYNAMIC CYPHER
    async def layer_3_graph_reasoning(self, query: str) -> str:
        schema = (
            "Nodes:\n"
            "- ChuongTrinhDaoTao(id, ma_chuong_trinh, khoa_hoc, loai_hinh, ngon_ngu, tong_tin_chi, thoi_gian): Thực thể trung tâm quản lý chương trình đào tạo.\n"
            "- HocPhan(id, ma_hp, ten_hp, so_tin_chi, so_tiet_ly_thuyet, so_tiet_thuc_hanh, bat_buoc): Chi tiết về môn học.\n"
            "- KhoiKienThuc(id, ten_khoi, tong_tin_chi, tin_chi_bat_buoc, tin_chi_tu_chon): Các khối như GD đại cương, Cơ sở ngành.\n"
            "- YeuCauTuChon(id, noi_dung_yeu_cau, mo_ta): Ràng buộc số tín chỉ tự chọn (VD: Chọn 10 TC).\n"
            "- NhomHocPhanTuChon(id, ten_nhom): Nhóm nhỏ môn học trong yêu cầu tự chọn.\n"
            "- ChuanDauRa(id, noi_dung, nhom, loai): Các chuẩn PLO/CDR phân theo Kiến thức/Kỹ năng/Thái độ.\n"
            "- VanBanPhapLy(id, so_hieu, ten_vb, loai_vb, ngay_ban_hanh, co_quan, noi_dung_goc): Quyết định ban hành hoặc văn bản căn cứ.\n"
            "- Nganh(id, ten_vi, ten_en): Thông tin ngành đào tạo.\n"
            "- Khoa(id, ten) & BoMon(id, ten): Đơn vị quản lý chuyên môn.\n"
            "- TrinhDo(id, ten): Cấp bậc đào tạo (Đại học/Cao đẳng).\n"
            "- LoaiVanBang(id, ten): Danh hiệu văn bằng (Kỹ sư/Cử nhân).\n"
            "- HinhThucDaoTao(id, ten) & PhuongThucDaoTao(id, ten): Cách thức tổ chức đào tạo.\n"
            "- MucTieuDaoTao(id, loai, noi_dung): Mục tiêu PEOs chung hoặc cụ thể.\n"
            "- ViTriViecLam(id, loai, noi_dung): Cơ hội nghề nghiệp sau tốt nghiệp.\n"
            "- ChuanThamKhao(id, noi_dung, link, noi_dung_goc): Tài liệu hoặc chuẩn tham chiếu quốc tế.\n"
            "- KhaNangHocTap(id, noi_dung): Định hướng học tập nâng cao.\n\n"

            "Edges:\n"
            "- (:ChuongTrinhDaoTao)-[:DUOC_BAN_HANH_THEO]->(:VanBanPhapLy)\n"
            "- (:ChuongTrinhDaoTao)-[:DAO_TAO_TRINH_DO]->(:TrinhDo)\n"
            "- (:ChuongTrinhDaoTao)-[:THUOC_VE]->(:Nganh)\n"
            "- (:Nganh)-[:THUOC_VE]->(:Khoa|:BoMon)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_LOAI_VAN_BANG]->(:LoaiVanBang)\n"
            "- (:ChuongTrinhDaoTao)-[:DAO_TAO_THEO_HINH_THUC]->(:HinhThucDaoTao)\n"
            "- (:ChuongTrinhDaoTao)-[:DAO_TAO_THEO_PHUONG_THUC]->(:PhuongThucDaoTao)\n"
            "- (:PhuongThucDaoTao)-[:CO_QUY_DINH]->(:VanBanPhapLy)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_MUC_TIEU_DAO_TAO]->(:MucTieuDaoTao)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_CO_HOI_VIEC_LAM]->(:ViTriViecLam)\n"
            "- (:ChuongTrinhDaoTao)-[:TUAN_THU]->(:VanBanPhapLy)\n"
            "- (:ChuongTrinhDaoTao)-[:THAM_CHIEU]->(:ChuanThamKhao)\n"
            "- (:ChuongTrinhDaoTao)-[:TAO_NEN_TANG]->(:KhaNangHocTap)\n"
            "- (:ChuongTrinhDaoTao)-[:DAT_CHUAN_DAU_RA]->(:ChuanDauRa)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_KHOI_KIEN_THUC]->(:KhoiKienThuc)\n"
            "- (:KhoiKienThuc)-[:CO_YEU_CAU_TU_CHON]->(:YeuCauTuChon)\n"
            "- (:YeuCauTuChon)-[:CO_NHOM_THANH_PHAN]->(:NhomHocPhanTuChon)\n"
            "- (:KhoiKienThuc|:YeuCauTuChon|:NhomHocPhanTuChon)-[:GOM_HOC_PHAN]->(:HocPhan)\n"
            "- (:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(:HocPhan)\n"
            "- (:HocPhan)-[:CO_THE_SONG_HANH]->(:HocPhan)\n"
        )

        system_prompt = (
            f"Bạn là một chuyên gia về cơ sở dữ liệu đồ thị Neo4j. Dựa trên schema sau đây:\n"
            f"{schema}\n\n"
            
            "### QUY TẮC TRUY VẤN:\n"
            "1. Ưu tiên dùng 'CONTAINS' và hàm 'toLower()' khi lọc tên để tránh lỗi chữ hoa/thường.\n"
            "2. Khi truy vấn về học phần, hãy nhớ 'HocPhan' có thể nối từ 'KhoiKienThuc' hoặc 'NhomHocPhanTuChon' qua quan hệ 'GOM_HOC_PHAN'.\n"
            "3. Chỉ trả về mã code Cypher trong khối ```cypher, không giải thích gì thêm.\n\n"

            "### VÍ DỤ FEW-SHOT:\n"
            
            "# Ví dụ 1: Tìm môn tiên quyết\n"
            "User: 'Môn nào là tiên quyết của môn Cấu trúc dữ liệu?'\n"
            "Assistant:\n"
            "```cypher\n"
            "MATCH (target:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(pre:HocPhan)\n"
            "WHERE toLower(target.ten_hp) CONTAINS toLower('Cấu trúc dữ liệu')\n"
            "RETURN pre.ma_hp, pre.ten_hp\n"
            "```\n\n"

            "# Ví dụ 2: Tìm thông tin chương trình đào tạo của một ngành\n"
            "User: 'Chương trình đào tạo ngành An toàn thông tin khóa 50 có bao nhiêu tín chỉ?'\n"
            "Assistant:\n"
            "```cypher\n"
            "MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)\n"
            "WHERE n.ten_vi CONTAINS 'An toàn thông tin' AND ctdt.khoa_hoc = 50\n"
            "RETURN ctdt.tong_tin_chi\n"
            "```\n\n"

            "# Ví dụ 3: Truy vấn phức tạp về nhóm tự chọn (dựa trên logic ETL của bạn)\n"
            "User: 'Trong khối chuyên ngành, tôi phải chọn bao nhiêu tín chỉ tự chọn?'\n"
            "Assistant:\n"
            "```cypher\n"
            "MATCH (k:KhoiKienThuc)-[:CO_YEU_CAU_TU_CHON]->(y:YeuCauTuChon)\n"
            "WHERE toLower(k.ten_khoi) CONTAINS toLower('chuyên ngành')\n"
            "RETURN y.noi_dung_yeu_cau, y.mo_ta\n"
            "```\n\n"

            "### CÂU HỎI CỦA NGƯỜI DÙNG:\n"
        )
        
        llm_response = await self._call_llm(query, system_prompt)
        cypher_match = re.search(r'```cypher(.*?)```', llm_response, re.DOTALL)
        
        if cypher_match:
            cypher_code = cypher_match.group(1).strip()
            print(f"-> Thực thi Cypher: {cypher_code}")
            try:
                async with self.db.driver.session() as session:
                    result = await session.run(cypher_code)
                    data = await result.data()
                    if data:
                        return f"Kết quả truy vấn đồ thị: {json.dumps(data, ensure_ascii=False)}"
            except Exception as e:
                return f"Lỗi truy vấn đồ thị: {e}"
        return "Hiện chưa tìm thấy thông tin chi tiết trong đồ thị đào tạo."

    def detect_intent(self, query: str):
        q = query.lower()

        # 1. greeting
        if any(x in q for x in ["xin chào", "hello", "hi", "chào", "hey"]):
            return "GREETING"

        # 2. cảm ơn
        if any(x in q for x in ["cảm ơn", "thanks", "thank"]):
            return "THANKS"

        # 3. học phí
        if "học phí" in q:
            return "TUITION"

        # 4. môn tiên quyết
        if any(x in q for x in ["tiên quyết", "học trước"]):
            return "PREREQUISITE"

        # 5. tín chỉ
        if "tín chỉ" in q:
            return "CREDIT"

        # 5b. thông tin học phần tổng quát
        if any(x in q for x in ["môn học", "học phần", "mã môn"]):
            return "COURSE_INFO"

        # 6. chương trình đào tạo
        if any(x in q for x in ["chương trình", "ctdt", "khung chương trình"]):
            return "PROGRAM"

        # 7. mô tả ngành
        if any(x in q for x in ["ngành", "học gì", "mô tả", "giới thiệu"]):
            return "MAJOR_INFO"

        # 8. cơ hội việc làm
        if any(x in q for x in ["việc làm", "nghề", "ra trường", "làm gì"]):
            return "CAREER"

        # 9. chuẩn đầu ra
        if any(x in q for x in ["chuẩn đầu ra", "plo"]):
            return "OUTCOME"

        # 10. hình thức đào tạo
        if any(x in q for x in ["chính quy", "tại chức", "online"]):
            return "TRAINING_FORM"

        # 11. thời gian học
        if any(x in q for x in ["bao lâu", "mấy năm", "thời gian học"]):
            return "DURATION"

        # 12. khoa / bộ môn
        if any(x in q for x in ["khoa", "bộ môn"]):
            return "FACULTY"

        # 13. văn bằng
        if any(x in q for x in ["bằng gì", "văn bằng"]):
            return "DEGREE"

        # 14. học tiếp
        if any(x in q for x in ["học tiếp", "cao học", "thạc sĩ"]):
            return "FURTHER_STUDY"

        # 15. fallback
        return "GENERAL"

    async def get_context(self, query: str):
        pipeline_out = await self.run_main_pipeline(query, history=[])
        context = pipeline_out.get("context") or ""
        return context if context else None

        # 5. gọi LLM
        # return await self.final_synthesis(query, context)

# if __name__ == "__main__":
#     bot = GraphRAGChatbot()
#     try:
#         async def main():
#             context = await bot.get_context("Môn An ninh mạng cần học môn nào trước?")
#             print(f"[BOT]: {context}")
        
#         asyncio.run(main())
#     finally:
#         asyncio.run(bot.db.close())