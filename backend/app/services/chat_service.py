import asyncio
import json
import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

from app.db.neo4j import get_driver
from app.scripts.Embedding import EmbeddingModel
from app.scripts.schema import GRAPH_SCHEMA
from app.services.llm_service import LLMService, MODEL_PRIMARY_9B, call_model_9b, call_model_json
from app.services.quyche_service import quyche_service
from app.services.subgraph_retriever import GraphBuildResult, SubgraphRetriever

logger = logging.getLogger("app.chat")

_QCHV_HINTS = (
    "học lại",
    "hoc lai",
    "cảnh báo học vụ",
    "canh bao hoc vu",
    "đình chỉ",
    "dinh chi",
    "điểm rèn luyện",
    "diem ren luyen",
)

_CTDT_HINTS = (
    "môn",
    "mon",
    "tín chỉ",
    "tin chi",
    "học phần",
    "hoc phan",
    "chương trình",
    "chuong trinh",
    "ngành",
    "nganh",
)

_SOCIAL_HINTS = (
    "chào",
    "chao",
    "hello",
    "cảm ơn",
    "cam on",
)

_ALLOWED_VECTOR_TARGETS = {"HocPhan", "DieuKienTotNghiep", "ChuanDauRa", "VanBanPhapLy"}
_ALLOWED_GRAPH_RELATIONS = {
    "BAN_HANH_THEO",
    "DAO_TAO",
    "THUOC_VE",
    "CAP",
    "CO",
    "THAM_KHAO",
    "DAT_DUOC",
    "YEU_CAU",
    "GOM",
    "DOI_VOI",
    "YEU_CAU_TIEN_QUYET",
    "CO_THE_SONG_HANH",
}
_ALLOWED_ANCHOR_NODE_TYPES = {
    "ChuongTrinhDaoTao",
    "Nganh",
    "KhoiKienThuc",
    "HocPhan",
    "ChuanDauRa",
    "DieuKienTotNghiep",
    "VanBanPhapLy",
    "Khoa",
    "BoMon",
}
_ALLOWED_ANSWER_SHAPES = {"single", "list", "path", "summary"}

_VECTOR_INDEX_CANDIDATES = {
    "HocPhan": ["hoc_phan_vector_index", "hocphan_vector_index", "global_knowledge_index"],
    "DieuKienTotNghiep": ["dieu_kien_tot_nghiep_vector_index", "global_knowledge_index"],
    "ChuanDauRa": ["chuan_dau_ra_vector_index", "global_knowledge_index"],
    "VanBanPhapLy": ["van_ban_phap_ly_vector_index", "global_knowledge_index"],
}

_HUMANIZE_KEYS = {
    "ctdt": "CTĐT",
    "nganh": "Ngành",
    "khoa": "Khóa",
    "he": "Hệ",
    "labels": "Nhãn",
    "related": "Liên quan",
    "source": "Nguồn",
    "noi_dung": "Nội dung",
    "ten": "Tên",
    "so": "Số",
    "nhom": "Nhóm",
    "loai": "Loại",
    "so_tin_chi": "Tín chỉ",
    "ten_hoc_phan": "Tên học phần",
    "ma_hoc_phan": "Mã học phần",
    "tom_tat": "Tóm tắt",
    "total": "Tổng",
    "total_tin_chi": "Tổng tín chỉ",
    "path_labels": "Nhãn đường đi",
    "path_nodes": "Nút đường đi",
}


@dataclass
class ChatPipelineState:
    query: str
    needs_query: bool = True
    query_gate_reason: str = ""
    domain: str = ""
    domain_source: str = ""
    intent: str = ""
    entities: List[Dict[str, str]] = field(default_factory=list)
    strategy: str = ""
    top_k: int = 4
    rewritten_query: str = ""
    context: str = ""
    graph_context: str = ""
    vector_context: str = ""
    graph_query: str = ""
    graph_params: Dict[str, Any] = field(default_factory=dict)
    analysis: Dict[str, Any] = field(default_factory=dict)
    vector_targets: List[str] = field(default_factory=list)
    used_default_ctdt: bool = False
    retries: int = 0
    max_retries: int = 1


def _truncate_text(text: str, limit: int = 240) -> str:
    value = (text or "").strip()
    return value if len(value) <= limit else value[:limit] + "...<truncated>"


def _stringify(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value)


def _strip_embedding(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, item in value.items():
            if str(key).lower() == "embedding":
                continue
            cleaned[key] = _strip_embedding(item)
        return cleaned
    if isinstance(value, list):
        return [_strip_embedding(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_embedding(item) for item in value)
    return value


def _normalize_history(history: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    cleaned: List[Dict[str, str]] = []
    for msg in (history or [])[-8:]:
        role = str(msg.get("role", "user")).strip().lower()
        content = str(msg.get("content", "")).strip()
        if role not in {"user", "assistant"}:
            role = "user"
        if content:
            cleaned.append({"role": role, "content": content})
    return cleaned


def _history_text(history: List[Dict[str, str]]) -> str:
    return "\n".join(f"{item['role']}: {item['content']}" for item in history)


def _contains_any(text: str, keywords: Tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _rule_based_domain(query: str) -> Tuple[str, bool, str]:
    text = (query or "").strip()
    if not text:
        return "CTDT", False, "empty"

    lowered = text.lower()
    qchv_hit = _contains_any(lowered, _QCHV_HINTS)
    ctdt_hit = _contains_any(lowered, _CTDT_HINTS)
    social_hit = _contains_any(lowered, _SOCIAL_HINTS)

    if social_hit and not (qchv_hit or ctdt_hit):
        return "SOCIAL", True, "rule"
    if qchv_hit and not (ctdt_hit or social_hit):
        return "QCHV", True, "rule"
    if ctdt_hit and not (qchv_hit or social_hit):
        return "CTDT", True, "rule"
    if social_hit or qchv_hit or ctdt_hit:
        return "", False, "conflict"
    return "", False, "fallback"


def _normalize_domain_value(domain: str) -> str:
    value = (domain or "").strip().upper()
    if value in {"QCHV", "QUY_CHE", "QUY CHE", "QUYCHE"}:
        return "QCHV"
    if value in {"SOCIAL", "SOCIALIZE"}:
        return "SOCIAL"
    return "CTDT"


def _normalize_query_type(value: str) -> str:
    query_type = (value or "").strip().lower()
    if query_type in {"attribute", "relation", "path", "aggregation", "description", "list"}:
        return query_type
    return "description"


def _normalize_ctdt_filters(raw_filters: Dict[str, Any]) -> Dict[str, Any]:
    filters = raw_filters if isinstance(raw_filters, dict) else {}
    normalized: Dict[str, Any] = {}

    # `khoa` chỉ chấp nhận khóa tuyển sinh dạng số (51, 52, ...).
    khoa_raw = filters.get("khoa")
    khoa_value: Any = None
    if isinstance(khoa_raw, int):
        khoa_value = khoa_raw
    elif isinstance(khoa_raw, str):
        digits = re.findall(r"\d+", khoa_raw)
        if digits:
            try:
                khoa_value = int(digits[0])
            except Exception:
                khoa_value = None
    if khoa_value is not None:
        normalized["khoa"] = khoa_value

    he_raw = filters.get("he")
    if isinstance(he_raw, str):
        he_value = he_raw.strip()
        if he_value:
            normalized["he"] = he_value

    return normalized


def _normalize_analysis_payload(raw: Dict[str, Any], query: str) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    rewrite = str(data.get("rewrite") or data.get("rewritten_query") or query).strip() or query

    entities = data.get("entities")
    if not isinstance(entities, list):
        entities = []
    cleaned_entities: List[Dict[str, str]] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_type = str(entity.get("type", "")).strip()
        entity_value = str(entity.get("value", "")).strip()
        if entity_type or entity_value:
            cleaned_entities.append({"type": entity_type, "value": entity_value})

    relations = data.get("relations")
    if not isinstance(relations, list):
        relations = []
    cleaned_relations = [str(item).strip().upper() for item in relations if str(item).strip()]

    constraints = data.get("constraints")
    if not isinstance(constraints, dict):
        constraints = {}

    ctdt_filters = data.get("ctdt_filters")
    ctdt_filters = _normalize_ctdt_filters(ctdt_filters if isinstance(ctdt_filters, dict) else {})

    vector_targets = data.get("vector_targets")
    if not isinstance(vector_targets, list):
        vector_targets = []
    cleaned_vector_targets = [str(item).strip() for item in vector_targets if str(item).strip() in _ALLOWED_VECTOR_TARGETS]
    if len(cleaned_vector_targets) > 2:
        cleaned_vector_targets = cleaned_vector_targets[:2]

    graph_plan = _normalize_graph_plan(
        raw_plan=data.get("graph_plan"),
        entities=cleaned_entities,
        relations=cleaned_relations,
        query_type=_normalize_query_type(str(data.get("query_type", "")).strip()),
        constraints=constraints,
    )

    return {
        "rewrite": rewrite,
        "entities": cleaned_entities,
        "relations": cleaned_relations,
        "query_type": _normalize_query_type(str(data.get("query_type", "")).strip()),
        "constraints": constraints,
        "ctdt_filters": ctdt_filters,
        "vector_targets": cleaned_vector_targets,
        "graph_plan": graph_plan,
    }


def _normalize_graph_plan(
    raw_plan: Any,
    entities: List[Dict[str, str]],
    relations: List[str],
    query_type: str,
    constraints: Dict[str, Any],
) -> Dict[str, Any]:
    plan = raw_plan if isinstance(raw_plan, dict) else {}

    raw_anchor_types = plan.get("anchor_node_types") if isinstance(plan.get("anchor_node_types"), list) else []
    anchor_node_types = [
        str(item).strip()
        for item in raw_anchor_types
        if str(item).strip() in _ALLOWED_ANCHOR_NODE_TYPES
    ]
    if not anchor_node_types:
        for entity in entities:
            entity_type = str(entity.get("type", "")).strip()
            if entity_type in _ALLOWED_ANCHOR_NODE_TYPES and entity_type not in anchor_node_types:
                anchor_node_types.append(entity_type)

    raw_focus_relations = plan.get("focus_relations") if isinstance(plan.get("focus_relations"), list) else []
    focus_relations = [
        str(item).strip().upper()
        for item in raw_focus_relations
        if str(item).strip().upper() in _ALLOWED_GRAPH_RELATIONS
    ]
    if not focus_relations:
        focus_relations = [rel for rel in relations if rel in _ALLOWED_GRAPH_RELATIONS]

    raw_expansion = plan.get("expansion_policy") if isinstance(plan.get("expansion_policy"), dict) else {}
    max_depth = raw_expansion.get("max_depth", constraints.get("depth", 2))
    max_paths = raw_expansion.get("max_paths", 60)
    max_nodes = raw_expansion.get("max_nodes", 70)

    try:
        max_depth = int(max_depth)
    except Exception:
        max_depth = 2
    try:
        max_paths = int(max_paths)
    except Exception:
        max_paths = 60
    try:
        max_nodes = int(max_nodes)
    except Exception:
        max_nodes = 70

    answer_shape = str(plan.get("answer_shape", "")).strip().lower()
    if answer_shape not in _ALLOWED_ANSWER_SHAPES:
        if query_type == "path":
            answer_shape = "path"
        elif query_type in {"list", "relation"}:
            answer_shape = "list"
        elif query_type in {"attribute", "aggregation"}:
            answer_shape = "single"
        else:
            answer_shape = "summary"

    return {
        "anchor_node_types": anchor_node_types[:3],
        "focus_relations": focus_relations[:4],
        "expansion_policy": {
            "max_depth": max(1, min(max_depth, 2)),
            "max_paths": max(10, min(max_paths, 120)),
            "max_nodes": max(20, min(max_nodes, 120)),
        },
        "answer_shape": answer_shape,
    }


def _format_rows(rows: List[Dict[str, Any]], section_name: str) -> str:
    if not rows:
        return f"[{section_name}]\nKhông có kết quả."

    lines = [f"[{section_name}]"]
    for index, row in enumerate(rows, 1):
        row_parts: List[str] = []
        sanitized_row = _strip_embedding(row)
        for key, value in sanitized_row.items():
            label = _HUMANIZE_KEYS.get(key, key.replace("_", " ").title())
            row_parts.append(f"{label}: {_stringify(value)}")
        lines.append(f"{index}. {' | '.join(row_parts)}")
    return "\n".join(lines)


def _format_hybrid_context(graph_context: str, vector_context: str) -> str:
    graph_section = graph_context.strip() or "[GRAPH RESULT]\nKhông có kết quả graph."
    vector_section = vector_context.strip() or "[VECTOR RESULT]\nKhông có kết quả vector."
    return f"{graph_section}\n\n{vector_section}"


_embed_model: EmbeddingModel | None = None
_embed_model_lock = threading.Lock()


def _get_embed_model() -> EmbeddingModel:
    global _embed_model
    if _embed_model is not None:
        return _embed_model
    with _embed_model_lock:
        if _embed_model is not None:
            return _embed_model
        _embed_model = EmbeddingModel()
        return _embed_model


def _execute_vector_query(index_name: str, embedding: List[float], top_k: int = 3) -> List[Dict[str, Any]]:
    cypher = (
        f"CALL db.index.vector.queryNodes('{index_name}', $top_k, $embedding) "
        "YIELD node, score "
        "RETURN labels(node) AS labels, properties(node) AS node, score "
        "ORDER BY score DESC "
        "LIMIT $top_k"
    )
    driver = get_driver()
    with driver.session() as session:
        return session.run(cypher, embedding=embedding, top_k=top_k).data()


def _vector_index_candidates(target: str) -> List[str]:
    return _VECTOR_INDEX_CANDIDATES.get(target, ["global_knowledge_index"])


def _format_vector_rows(target: str, rows: List[Dict[str, Any]], index_name: str) -> str:
    if not rows:
        return ""
    lines = [f"- {target} [{index_name}]"]
    for index, row in enumerate(rows, 1):
        sanitized_row = _strip_embedding(row)
        score = sanitized_row.get("score")
        labels = sanitized_row.get("labels", [])
        node = sanitized_row.get("node", {})
        lines.append(f"  {index}. score={score} labels={_stringify(labels)} node={_stringify(node)}")
    return "\n".join(lines)


async def _search_vector_target(query: str, target: str, top_k: int = 3) -> str:
    try:
        embedding_batch = await asyncio.to_thread(_get_embed_model().get_embedding_batch, [query])
        if not embedding_batch:
            return ""
        embedding = embedding_batch[0]
    except Exception as exc:
        logger.warning("[chat][vector] embedding failed target=%s error=%s", target, exc)
        return ""

    for index_name in _vector_index_candidates(target):
        try:
            rows = await asyncio.to_thread(_execute_vector_query, index_name, embedding, top_k)
        except Exception as exc:
            logger.info("[chat][vector] index=%s unavailable target=%s error=%s", index_name, target, exc)
            continue
        if rows:
            return _format_vector_rows(target, rows, index_name)
    return ""



class ChatService:
    def __init__(self) -> None:
        self.llm_service = LLMService()
        self.subgraph_retriever = SubgraphRetriever()

    async def handle_query(self, query: str, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Điều phối pipeline hybrid GraphRAG."""
        state = ChatPipelineState(query=(query or "").strip())
        cleaned_history = _normalize_history(history)
        history_text = _history_text(cleaned_history)

        if not state.query:
            logger.info("[chat][pipeline][empty_query] routing to generate_without_query")
            answer = await self.llm_service.generate_without_query(state.query, history_text)
            logger.info("[chat][pipeline][done] path=empty_query answer=%s", answer)
            return {"answer": answer, "state": state.__dict__}

        domain, matched_by_rule, reason = _rule_based_domain(state.query)
        state.domain = domain
        state.domain_source = reason
        state.query_gate_reason = reason

        logger.info(
            "[chat][pipeline][domain_gate] query=%s domain=%s matched_by_rule=%s reason=%s",
            state.query,
            state.domain,
            matched_by_rule,
            reason,
        )

        if not matched_by_rule:
            logger.info("[chat][pipeline][domain_llm] query=%s history=%s", state.query, history_text or "[empty]")
            state.domain = await self._classify_domain_with_llm(state.query, history_text)
            state.domain_source = "llm"
            logger.info("[chat][pipeline][domain_llm_result] domain=%s", state.domain)

        if state.domain == "SOCIAL":
            state.needs_query = False
            logger.info("[chat][pipeline][social] query=%s history=%s", state.query, history_text or "[empty]")
            answer = await self.llm_service.generate_without_query(state.query, history_text)
            logger.info("[chat][pipeline][done] path=social answer=%s", answer)
            return {"answer": answer, "state": state.__dict__}

        if state.domain == "QCHV":
            logger.info("[chat][pipeline][qchv] query=%s history=%s", state.query, history_text or "[empty]")
            try:
                result = await quyche_service.handle_query(state.query, cleaned_history)
                answer = str(result.get("answer", "")).strip()
                if not answer:
                    answer = "Mình chưa tìm được đủ dữ liệu quy chế để trả lời chính xác."
            except Exception:
                logger.exception("[chat][pipeline][qchv_error] fallback_to_stub")
                answer = await self.handle_quy_che_stub(state, cleaned_history)
            logger.info("[chat][pipeline][done] path=qchv answer=%s", answer)
            return {"answer": answer, "state": state.__dict__}

        analysis = await self._analyze_ctdt_query(state.query, history_text)
        state.analysis = analysis
        state.rewritten_query = analysis.get("rewrite", state.query)
        state.intent = analysis.get("query_type", "description")
        state.entities = analysis.get("entities", [])
        state.vector_targets = analysis.get("vector_targets", []) or ["HocPhan", "DieuKienTotNghiep"]

        retrieval_analysis = dict(analysis)
        retrieval_analysis["query"] = state.query
        graph_result = await asyncio.to_thread(self.subgraph_retriever.retrieve, retrieval_analysis)

        state.graph_query = "subgraph_retrieval"
        state.graph_params = graph_result.metadata
        state.used_default_ctdt = graph_result.used_default_ctdt
        state.strategy = "graph_first_subgraph"

        graph_context, vector_context = await self._retrieve_hybrid_context(
            state.rewritten_query,
            graph_result,
            state.vector_targets,
        )
        state.graph_context = graph_context
        state.vector_context = vector_context
        state.context = _format_hybrid_context(graph_context, vector_context)

        logger.info(
            "[chat][pipeline][ctdt_analysis_result] analysis=%s",
            analysis,
        )
        logger.info(
            "[chat][pipeline][graph_build] mode=%s metadata=%s used_default_ctdt=%s is_graph_sufficient=%s",
            state.graph_query,
            state.graph_params,
            state.used_default_ctdt,
            graph_result.is_graph_sufficient,
        )
        logger.info(
            "[chat][pipeline][retrieval_start] rewritten_query=%s vector_targets=%s",
            state.rewritten_query,
            state.vector_targets,
        )
        logger.info("[chat][pipeline][hybrid_context]\n%s", state.context)

        logger.info(
            "[chat][pipeline][answer_start] original_query=%s rewritten_query=%s used_default_ctdt=%s",
            state.query,
            state.rewritten_query,
            state.used_default_ctdt,
        )

        answer = await self._generate_answer(
            original_query=state.query,
            rewritten_query=state.rewritten_query,
            context=state.context,
            used_default_ctdt=state.used_default_ctdt,
        )
        logger.info("[chat][pipeline][answer_done] answer=%s", answer)
        logger.info("[chat][pipeline][done] path=ctdt")
        return {"answer": answer, "state": state.__dict__}

    async def _classify_domain_with_llm(self, query: str, history_text: str) -> str:
        logger.info("[chat][pipeline][domain_llm_prompt] query=%s history=%s", query, history_text or "[empty]")
        prompt = f"""
Bạn là bộ phân loại domain cho chatbot tư vấn học vụ.

Quy tắc output:
- Chỉ trả về JSON đúng format: {{"domain": "QCHV | CTDT | SOCIAL"}}
- Không giải thích.

Lưu ý:
- QCHV: quy chế học vụ, học lại, cảnh báo học vụ, đình chỉ, điểm rèn luyện.
- CTDT: môn học, tín chỉ, học phần, chương trình, ngành.
- SOCIAL: chào hỏi, cảm ơn, hello.

History:
{history_text or "[empty]"}

Query:
{query}
"""
        data = await call_model_json(MODEL_PRIMARY_9B, prompt)
        domain = _normalize_domain_value(str(data.get("domain", "CTDT")))
        logger.info("[chat][pipeline][domain_llm_output] raw=%s normalized=%s", data, domain)
        return domain

    async def _analyze_ctdt_query(self, query: str, history_text: str) -> Dict[str, Any]:
        prompt = f"""
Bạn là chuyên gia phân tích truy vấn CTDT cho hệ thống GraphRAG.
Phải trả về JSON STRICT với format sau:
{{
  "rewrite": "...",
  "entities": [{{"type": "...", "value": "..."}}],
  "relations": ["..."],
  "query_type": "attribute | relation | path | aggregation | description | list",
  "constraints": {{"depth": 2, "limit": 10}},
  "ctdt_filters": {{"khoa": 51, "he": "đại trà"}},
    "vector_targets": ["HocPhan", "DieuKienTotNghiep", "ChuanDauRa", "VanBanPhapLy"],
    "graph_plan": {{
            "anchor_node_types": ["HocPhan", "Nganh", "ChuongTrinhDaoTao"],
            "focus_relations": ["YEU_CAU_TIEN_QUYET", "CO_THE_SONG_HANH", "GOM", "CO", "YEU_CAU", "THUOC_VE"],
            "expansion_policy": {{"max_depth": 2, "max_paths": 60, "max_nodes": 70}},
            "answer_shape": "single | list | path | summary"
    }}
}}

Ràng buộc bắt buộc:
- Chỉ dùng 1 lần gọi LLM.
- rewrite phải là câu hỏi độc lập, rõ nghĩa, dùng lịch sử chat nếu cần.
- entities là danh sách linh hoạt, chỉ chứa thực thể thật xuất hiện hoặc suy ra trực tiếp từ câu hỏi.
- relations chỉ lấy từ schema, nếu không chắc thì []
- query_type chọn 1 giá trị duy nhất.
- Định nghĩa query_type:
    + attribute: hỏi 1 thuộc tính cụ thể của 1 thực thể (ví dụ: tong_tin_chi của ChuongTrinhDaoTao, so_tin_chi của HocPhan).
    + relation: hỏi các thực thể liên quan qua 1 quan hệ (ví dụ: CTDT có những ChuanDauRa nào).
    + path: hỏi đường đi/chuỗi quan hệ giữa các thực thể.
    + aggregation: hỏi thống kê tổng hợp nhiều bản ghi (count/sum/group), KHÔNG dùng cho thuộc tính đơn lẻ của 1 thực thể.
    + description: câu mô tả chung, chưa đủ tín hiệu để chọn nhánh cụ thể.
    + list: yêu cầu liệt kê danh sách (ưu tiên trả nhiều bản ghi).
- Với câu hỏi "tổng số tín chỉ cần hoàn thành của CTDT" thì ưu tiên query_type="attribute" và trọng tâm thuộc tính ct.tong_tin_chi.
- constraints chỉ điền khi thực sự cần; nếu không có thì {{}}
- ctdt_filters chỉ chứa giá trị được nêu rõ; KHÔNG tự thêm default khoa/he.
- vector_targets chỉ chọn trong tập: HocPhan, DieuKienTotNghiep, ChuanDauRa, VanBanPhapLy.
- Nếu không chắc thì vector_targets = []
- graph_plan phải tương thích schema:
    + anchor_node_types chỉ dùng label có trong schema.
    + focus_relations chỉ dùng quan hệ có trong schema.
    + answer_shape mô tả dạng đầu ra mong muốn, không ảnh hưởng trực tiếp tới Cypher.

Schema:
{GRAPH_SCHEMA}

History:
{history_text or "[empty]"}

Query:
{query}
"""
        raw = await call_model_json(MODEL_PRIMARY_9B, prompt)
        normalized = _normalize_analysis_payload(raw, query)
        if not normalized.get("vector_targets"):
            normalized["vector_targets"] = []
        return normalized

    async def _retrieve_hybrid_context(
        self,
        query: str,
        graph_result: GraphBuildResult,
        vector_targets: List[str],
    ) -> tuple[str, str]:
        graph_context = graph_result.graph_text.strip() or "[GRAPH RESULT]\nKhông có dữ liệu phù hợp."

        should_fallback_to_vector = not graph_result.is_graph_sufficient
        selected_targets = vector_targets if vector_targets else ["HocPhan", "DieuKienTotNghiep"]

        filtered_vector_sections: List[str] = []
        if should_fallback_to_vector and selected_targets:
            vector_sections = await asyncio.gather(*[
                self._search_vector_section(query, target)
                for target in selected_targets
            ])
            filtered_vector_sections = [section for section in vector_sections if section]

        vector_context = (
            "[VECTOR RESULT]\n" + "\n\n".join(filtered_vector_sections)
            if filtered_vector_sections
            else "[VECTOR RESULT]\nKhông có kết quả vector."
        )
        return graph_context, vector_context

    async def _search_vector_section(self, query: str, target: str) -> str:
        if target not in _ALLOWED_VECTOR_TARGETS:
            return ""
        result = await _search_vector_target(query, target, top_k=3)
        logger.info("[chat][pipeline][vector_target_result] target=%s found=%s", target, bool(result))
        return result

    async def _generate_answer(
        self,
        original_query: str,
        rewritten_query: str,
        context: str,
        used_default_ctdt: bool,
    ) -> str:
        default_note = "Hệ thống đã áp dụng mặc định CTĐT: khoa=51, he=đại trà." if used_default_ctdt else "[Không dùng default CTĐT]"
        logger.info(
            "[chat][pipeline][answer_prompt] original_query=%s rewritten_query=%s used_default_ctdt=%s",
            original_query,
            rewritten_query,
            used_default_ctdt,
        )
        prompt = f"""
Bạn là trợ lý tư vấn học vụ bằng tiếng Việt cho hệ thống GraphRAG.

Nhiệm vụ:
1. Phân tích context để xác định thông tin đủ hay chưa đủ.
2. Trả lời trực tiếp bằng tiếng Việt, ưu tiên fact từ GRAPH, dùng VECTOR để giải thích hoặc bổ sung bối cảnh.

Quy tắc:
- Không bịa thông tin ngoài context.
- Graph là nguồn sự thật chính.
- Vector chỉ là hỗ trợ diễn giải.
- Nếu context chưa đủ, nói rõ chưa đủ và nêu phần thiếu.
- Nếu có default CTĐT đã được dùng, nhắc ngắn gọn.

Thông tin bổ sung:
{default_note}

Câu hỏi gốc:
{original_query}

Câu hỏi đã chuẩn hóa:
{rewritten_query}

Context:
{context}

Hãy trả lời ngắn gọn, chính xác, và chỉ dùng thông tin trong context.
"""
        answer = await call_model_9b(prompt, temperature=0.2)
        logger.info("[chat][pipeline][answer_model_output] answer=%s", answer)
        return (answer or "").strip()
