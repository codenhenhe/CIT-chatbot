import asyncio
import json
import logging
import os
import re
import threading
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from app.db.neo4j import get_driver
from app.scripts.Embedding import EmbeddingModel


logger = logging.getLogger("app.retrieval")


def _truncate_text(text: str, limit: int = 2000) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "...<truncated>"


_embed_model: Optional[EmbeddingModel] = None
_embed_model_lock = threading.Lock()
_embedding_cache: Dict[str, List[float]] = {}
_source_metrics_lock = threading.Lock()
_source_metrics: Dict[str, float] = {
    "requests": 0,
    "graph": 0,
    "vector": 0,
    "none": 0,
    "started_at": time.time(),
}


def _log_source_metrics(source: str):
    with _source_metrics_lock:
        _source_metrics["requests"] += 1
        if source in ["graph", "vector", "none"]:
            _source_metrics[source] += 1

        req = int(_source_metrics["requests"])
        graph = int(_source_metrics["graph"])
        vector = int(_source_metrics["vector"])
        none = int(_source_metrics["none"])
        uptime_s = max(int(time.time() - float(_source_metrics["started_at"])), 1)

        graph_rate = graph / req
        vector_rate = vector / req
        none_rate = none / req

    print(
        "[retrieval] "
        f"source={source} "
        f"requests={req} "
        f"graph_rate={graph_rate:.3f} "
        f"vector_rate={vector_rate:.3f} "
        f"none_rate={none_rate:.3f} "
        f"uptime_s={uptime_s}"
    )


def _get_embed_model() -> EmbeddingModel:
    global _embed_model
    if _embed_model is not None:
        return _embed_model

    with _embed_model_lock:
        if _embed_model is None:
            model_name = os.getenv("EMBEDDING_MODEL_NAME", "BAAI/bge-m3")
            cache_folder = os.getenv("EMBEDDING_CACHE_FOLDER", "../my_model_weights/bge_m3")
            _embed_model = EmbeddingModel(model_name=model_name, cache_folder=cache_folder)
    return _embed_model


def _normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    s = text.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("đ", "d")
    s = re.sub(r"\s+", " ", s)
    return s


def extract_entities(question: str) -> Dict[str, Any]:
    q = (question or "").strip()
    q_norm = _normalize_text(q)

    result: Dict[str, Any] = {
        "nganh": None,
        "loai_hinh": None,
        "attribute": None,
        "intent": "ask_info",
    }

    major_match = re.search(
        r"ng[aà]nh\s+([^?.!,]+?)(?=\s+(?:c[oó]|kh[oô]ng|l[aà]|bao|m[aá]y|n[aà]o|clc|ch[aấ]t|đại|kh[oó]a|[!?.])|(?:c[oó]|[!?.])|$)",
        q,
        re.IGNORECASE,
    )
    if major_match:
        result["nganh"] = major_match.group(1).strip()

    if any(x in q_norm for x in ["clc", "chat luong cao", "chất lượng cao"]):
        result["loai_hinh"] = "clc"
    elif any(x in q_norm for x in ["dai tra", "đại trà"]):
        result["loai_hinh"] = "đại trà"

    if any(x in q_norm for x in ["tin chi", "tín chỉ"]):
        result["attribute"] = "tin_chi"
    elif any(x in q_norm for x in ["thoi gian", "thời gian", "bao lau", "mấy năm", "may nam"]):
        result["attribute"] = "thoi_gian"
    elif any(x in q_norm for x in ["muc tieu", "mục tiêu"]):
        result["attribute"] = "muc_tieu"
    elif any(x in q_norm for x in ["hoc phan", "học phần", "mon hoc", "môn học"]):
        result["attribute"] = "hoc_phan"

    if any(x in q_norm for x in ["bao nhieu tin chi", "so tin chi", "tong tin chi"]):
        result["intent"] = "ask_credit"
    elif any(x in q_norm for x in ["co may nganh", "bao nhieu nganh", "nhung nganh", "liet ke cac nganh"]):
        result["intent"] = "ask_major_list"
    elif any(x in q_norm for x in ["chuong trinh dao tao", "ctdt", "chương trình"]):
        result["intent"] = "ask_program"
    elif any(x in q_norm for x in ["clc", "chat luong cao", "dai tra", "loai hinh"]):
        result["intent"] = "ask_program_type"
    elif any(x in q_norm for x in ["thoi gian", "thời gian", "mấy năm", "may nam"]):
        result["intent"] = "ask_duration"

    return result


def build_template_cypher(entities: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any]]:
    nganh = entities.get("nganh")
    loai_hinh = entities.get("loai_hinh")
    intent = entities.get("intent")

    if intent == "ask_major_list":
        return (
            """
MATCH (n:Nganh)
RETURN DISTINCT n.ten_nganh_vi as nganh, n.ten_nganh_en as nganh_en
ORDER BY nganh
LIMIT 20
""".strip(),
            {},
        )

    if intent == "ask_credit" and nganh:
        cypher = (
            """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($nganh)
   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($nganh)
""".strip()
        )
        if loai_hinh:
            cypher += "\nAND toLower(coalesce(ctdt.loai_hinh, '')) = toLower($loai_hinh)"
        cypher += (
            """
RETURN n.ten_nganh_vi as nganh, ctdt.khoa as khoa, ctdt.tong_tin_chi as tong_tin_chi
ORDER BY ctdt.khoa DESC
LIMIT 3
""".strip()
        )
        params = {"nganh": nganh, "loai_hinh": loai_hinh}
        return cypher, params

    if intent == "ask_program" and nganh:
        cypher = (
            """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($nganh)
   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($nganh)
""".strip()
        )
        if loai_hinh:
            cypher += "\nAND toLower(coalesce(ctdt.loai_hinh, '')) = toLower($loai_hinh)"
        cypher += (
            """
RETURN n.ten_nganh_vi as nganh, ctdt.khoa as khoa, ctdt.loai_hinh as loai_hinh, ctdt.thoi_gian_dao_tao as thoi_gian_dao_tao
ORDER BY ctdt.khoa DESC
LIMIT 5
""".strip()
        )
        params = {"nganh": nganh, "loai_hinh": loai_hinh}
        return cypher, params

    if intent == "ask_duration" and nganh:
        cypher = (
            """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(n.ten_nganh_vi, '')) CONTAINS toLower($nganh)
   OR toLower(coalesce(n.ten_nganh_en, '')) CONTAINS toLower($nganh)
RETURN n.ten_nganh_vi as nganh, ctdt.khoa as khoa, ctdt.thoi_gian_dao_tao as thoi_gian_dao_tao
ORDER BY ctdt.khoa DESC
LIMIT 3
""".strip()
        )
        return cypher, {"nganh": nganh}

    if intent == "ask_program_type" and loai_hinh == "clc":
        cypher = (
            """
MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
WHERE toLower(coalesce(ctdt.loai_hinh, '')) = toLower($loai_hinh)
RETURN DISTINCT n.ten_nganh_vi as nganh, n.ten_nganh_en as nganh_en, ctdt.loai_hinh as loai_hinh
ORDER BY nganh
LIMIT 20
""".strip()
        )
        return cypher, {"loai_hinh": loai_hinh}

    return None, {}


def _is_safe_cypher(cypher: Optional[str]) -> bool:
    if not cypher:
        return False
    clean = cypher.strip()
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
    if not (upper.startswith("MATCH") or upper.startswith("OPTIONAL MATCH")):
        return False
    if "RETURN" not in upper:
        return False
    return True


def _run_read_query(cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, **params)
        return result.data()


def _get_query_embedding(question: str) -> Optional[List[float]]:
    key = (question or "").strip()
    if not key:
        return None

    cached = _embedding_cache.get(key)
    if cached is not None:
        return cached

    model = _get_embed_model()
    vectors = model.get_embedding_batch([key])
    if not vectors:
        return None
    vec = vectors[0]
    _embedding_cache[key] = vec
    return vec


def _run_vector_query(vector: List[float], top_k: int, min_score: float) -> List[Dict[str, Any]]:
    cypher = """
CALL db.index.vector.queryNodes('global_knowledge_index', $k, $vector)
YIELD node, score
WHERE score >= $min_score
RETURN labels(node)[0] as type,
       coalesce(
           toString(node.text),
           toString(node.noi_dung),
           toString(node.ten_nganh_vi),
           toString(node.ten_hp),
           toString(node.ma_hp),
           'node:' + coalesce(toString(node.id), elementId(node))
       ) as context,
       score
ORDER BY score DESC
LIMIT $k
""".strip()

    return _run_read_query(
        cypher,
        {
            "vector": vector,
            "k": int(top_k),
            "min_score": float(min_score),
        },
    )


def _rows_to_context(rows: List[Dict[str, Any]], max_rows: int = 10) -> str:
    lines: List[str] = []
    for row in rows[:max_rows]:
        if not isinstance(row, dict):
            continue
        parts = []
        for key, value in row.items():
            if value is None or value == "":
                continue
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value if v is not None and str(v).strip())
            parts.append(f"{key}: {value}")
        if parts:
            lines.append("- " + "; ".join(parts))
    return "\n".join(lines)


def _vector_rows_to_context(rows: List[Dict[str, Any]], max_rows: int = 4) -> str:
    lines: List[str] = []
    seen: set = set()
    for row in rows[:max_rows]:
        text = str(row.get("context") or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        item_type = str(row.get("type") or "Entity").strip()
        score = float(row.get("score") or 0.0)
        lines.append(f"- [{item_type}|{score:.3f}] {text}")
    return "\n".join(lines)


def _merge_context(graph_context: str, vector_context: str) -> str:
    sections: List[str] = []
    if graph_context:
        sections.append(f"[GRAPH]\n{graph_context}")
    if vector_context:
        sections.append(f"[VECTOR]\n{vector_context}")
    return "\n\n".join(sections).strip()


def warmup_embedding_model() -> None:
    model = _get_embed_model()
    # Warm up one short query to avoid first-request latency spike.
    vectors = model.get_embedding_batch(["warmup"])
    if vectors:
        _embedding_cache.setdefault("warmup", vectors[0])


async def retrieve_graph_context(question: str) -> str:
    entities = extract_entities(question)
    cypher, params = build_template_cypher(entities)
    logger.info(
        "[retrieval][request] question=%s entities=%s",
        _truncate_text(question, 500),
        json.dumps(entities, ensure_ascii=False),
    )

    graph_rows: List[Dict[str, Any]] = []
    if cypher and _is_safe_cypher(cypher):
        logger.info(
            "[retrieval][graph_query] cypher=%s params=%s",
            _truncate_text(cypher, 2500),
            json.dumps(params, ensure_ascii=False),
        )
        try:
            graph_rows = await asyncio.to_thread(_run_read_query, cypher, params)
        except Exception:
            logger.exception("[retrieval][graph_query] Failed")
            graph_rows = []
        logger.info(
            "[retrieval][graph_rows] count=%d rows=%s",
            len(graph_rows),
            _truncate_text(json.dumps(graph_rows, ensure_ascii=False, default=str), 2500),
        )
    elif cypher:
        logger.warning("[retrieval][graph_query] blocked_by_safety cypher=%s", _truncate_text(cypher, 1200))
    else:
        logger.info("[retrieval][graph_query] no_template_cypher")

    graph_context = _rows_to_context(graph_rows, max_rows=10) if graph_rows else ""

    # Hybrid fallback: template graph first, then BGE vector search when graph is empty.
    vector_context = ""
    if not graph_context:
        query_vec = await asyncio.to_thread(_get_query_embedding, question)
        if query_vec is not None:
            logger.info(
                "[retrieval][vector_query] top_k=%d min_score=%.2f vector_dim=%d",
                4,
                0.68,
                len(query_vec),
            )
            vector_rows = await asyncio.to_thread(_run_vector_query, query_vec, 4, 0.68)
            logger.info(
                "[retrieval][vector_rows] count=%d rows=%s",
                len(vector_rows),
                _truncate_text(json.dumps(vector_rows, ensure_ascii=False, default=str), 2500),
            )
            vector_context = _vector_rows_to_context(vector_rows, max_rows=4)
        else:
            logger.info("[retrieval][vector_query] skipped_empty_embedding")

    if graph_context:
        source = "graph"
    elif vector_context:
        source = "vector"
    else:
        source = "none"

    merged_context = _merge_context(graph_context, vector_context)
    logger.info(
        "[retrieval][context] source=%s graph_context=%s vector_context=%s merged_context=%s",
        source,
        _truncate_text(graph_context, 2000),
        _truncate_text(vector_context, 2000),
        _truncate_text(merged_context, 3000),
    )

    _log_source_metrics(source)
    return merged_context
