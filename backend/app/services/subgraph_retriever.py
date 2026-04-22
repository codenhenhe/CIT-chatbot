from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from app.db.neo4j import get_driver

DEFAULT_KHOA = 51
DEFAULT_HE = "đại trà"

_NAME_KEYS = (
    "ten_hoc_phan",
    "ten_nganh_vi",
    "ten_khoi",
    "ten",
    "noi_dung",
    "ma_hoc_phan",
    "ma_chuong_trinh",
)

_PROP_KEYS = (
    "ma_hoc_phan",
    "ten_hoc_phan",
    "so_tin_chi",
    "ma_nganh",
    "ten_nganh_vi",
    "khoa",
    "he",
    "tong_tin_chi",
    "ten_khoi",
    "loai",
    "nhom",
    "ten",
)

_SEARCHABLE_PROP_KEYS = tuple(dict.fromkeys(_NAME_KEYS + _PROP_KEYS))

_ALLOWED_RELATION_HINTS = {
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


def _is_embedding_key(key: str) -> bool:
    lowered = str(key or "").strip().lower()
    return "embedding" in lowered or lowered.endswith("_vector") or lowered == "vector"


def _sanitize_props(props: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in (props or {}).items():
        if _is_embedding_key(key):
            continue
        cleaned[key] = value
    return cleaned


@dataclass
class GraphBuildResult:
    graph_text: str
    metadata: Dict[str, Any]
    used_default_ctdt: bool
    is_graph_sufficient: bool


class SubgraphRetriever:
    def __init__(
        self,
        seed_limit: int = 5,
        max_depth: int = 2,
        path_limit: int = 60,
        max_nodes: int = 70,
        max_relationships: int = 140,
        max_text_chars: int = 12000,
    ) -> None:
        self.seed_limit = max(1, min(seed_limit, 8))
        self.max_depth = max(1, min(max_depth, 2))
        self.path_limit = max(10, min(path_limit, 120))
        self.max_nodes = max(20, min(max_nodes, 120))
        self.max_relationships = max(30, min(max_relationships, 220))
        self.max_text_chars = max(3000, min(max_text_chars, 20000))

    def retrieve(self, analysis: Dict[str, Any]) -> GraphBuildResult:
        rewrite = str(analysis.get("rewrite") or analysis.get("query") or "").strip()
        entities = analysis.get("entities") if isinstance(analysis.get("entities"), list) else []
        ctdt_filters = analysis.get("ctdt_filters") if isinstance(analysis.get("ctdt_filters"), dict) else {}
        plan = self._normalize_graph_plan(analysis.get("graph_plan"), analysis)
        relation_hints = plan["focus_relations"]
        anchor_node_types = plan["anchor_node_types"]
        expansion_policy = plan["expansion_policy"]

        filters, used_default = self._normalize_filters(ctdt_filters)
        keywords = self._build_keywords(rewrite, entities)

        if not keywords:
            return GraphBuildResult(
                graph_text="[GRAPH RESULT]\nKhông có dữ liệu phù hợp.",
                metadata={"reason": "empty_keywords", "keywords": []},
                used_default_ctdt=used_default,
                is_graph_sufficient=False,
            )

        seed_rows = self._fetch_seed_nodes(keywords, filters)
        seed_ids = self._prioritize_seed_ids(seed_rows, anchor_node_types)

        if not seed_ids:
            return GraphBuildResult(
                graph_text="[GRAPH RESULT]\nKhông có dữ liệu phù hợp.",
                metadata={
                    "reason": "no_seed",
                    "keywords": keywords,
                    "seed_count": 0,
                    "filters": filters,
                },
                used_default_ctdt=used_default,
                is_graph_sufficient=False,
            )

        nodes_by_id, rels_by_id = self._expand_subgraph(seed_ids, filters, relation_hints, expansion_policy)
        graph_text = self.serialize_subgraph(nodes_by_id, rels_by_id)
        sufficient = self._is_graph_sufficient(nodes_by_id, rels_by_id, graph_text)

        return GraphBuildResult(
            graph_text=graph_text,
            metadata={
                "keywords": keywords,
                "filters": filters,
                "seed_count": len(seed_ids),
                "node_count": len(nodes_by_id),
                "relationship_count": len(rels_by_id),
                "max_depth": expansion_policy["max_depth"],
                "relation_hints": relation_hints,
                "anchor_node_types": anchor_node_types,
                "answer_shape": plan["answer_shape"],
            },
            used_default_ctdt=used_default,
            is_graph_sufficient=sufficient,
        )

    def _normalize_graph_plan(self, raw_plan: Any, analysis: Dict[str, Any]) -> Dict[str, Any]:
        plan = raw_plan if isinstance(raw_plan, dict) else {}

        raw_anchor_types = plan.get("anchor_node_types") if isinstance(plan.get("anchor_node_types"), list) else []
        anchor_node_types = [
            str(item).strip()
            for item in raw_anchor_types
            if str(item).strip() in _ALLOWED_ANCHOR_NODE_TYPES
        ]
        if not anchor_node_types:
            entities = analysis.get("entities") if isinstance(analysis.get("entities"), list) else []
            for entity in entities:
                entity_type = str((entity or {}).get("type", "")).strip()
                if entity_type in _ALLOWED_ANCHOR_NODE_TYPES and entity_type not in anchor_node_types:
                    anchor_node_types.append(entity_type)

        focus_relations = self._normalize_relation_hints(plan.get("focus_relations"))
        if not focus_relations:
            focus_relations = self._normalize_relation_hints(analysis.get("relations"))

        raw_expansion = plan.get("expansion_policy") if isinstance(plan.get("expansion_policy"), dict) else {}
        max_depth = raw_expansion.get("max_depth", self.max_depth)
        max_paths = raw_expansion.get("max_paths", self.path_limit)
        max_nodes = raw_expansion.get("max_nodes", self.max_nodes)

        try:
            max_depth = int(max_depth)
        except Exception:
            max_depth = self.max_depth
        try:
            max_paths = int(max_paths)
        except Exception:
            max_paths = self.path_limit
        try:
            max_nodes = int(max_nodes)
        except Exception:
            max_nodes = self.max_nodes

        answer_shape = str(plan.get("answer_shape", "summary")).strip().lower()
        if answer_shape not in {"single", "list", "path", "summary"}:
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

    def _normalize_relation_hints(self, raw_relations: Any) -> List[str]:
        if not isinstance(raw_relations, list):
            return []
        hints: List[str] = []
        for rel in raw_relations:
            relation = str(rel or "").strip().upper()
            if relation in _ALLOWED_RELATION_HINTS and relation not in hints:
                hints.append(relation)
        return hints[:4]

    def _normalize_filters(self, raw_filters: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        filters = dict(raw_filters or {})
        used_default = False

        khoa = filters.get("khoa")
        if khoa is None:
            filters["khoa"] = DEFAULT_KHOA
            used_default = True

        he = filters.get("he")
        if not he:
            filters["he"] = DEFAULT_HE
            used_default = True

        return filters, used_default

    def _build_keywords(self, rewrite: str, entities: List[Dict[str, Any]]) -> List[str]:
        keywords: List[str] = []

        if rewrite:
            keywords.append(rewrite)

        for entity in entities:
            value = str((entity or {}).get("value", "")).strip()
            if value:
                keywords.append(value)

        deduped: List[str] = []
        seen = set()
        for kw in keywords:
            norm = kw.strip().lower()
            if not norm or norm in seen:
                continue
            seen.add(norm)
            deduped.append(kw.strip())

        return deduped[:8]

    def _fetch_seed_nodes(self, keywords: List[str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        cypher = """
        UNWIND $keywords AS kw
        MATCH (n)
        WHERE kw <> ''
                    AND ANY(prop IN $search_props
                            WHERE prop IN keys(n)
                                AND n[prop] IS NOT NULL
                                AND toLower(toString(n[prop])) CONTAINS toLower(kw)
                    )
          AND EXISTS {
            MATCH (ct:ChuongTrinhDaoTao)-[*0..2]-(n)
            WHERE ($khoa IS NULL OR toString(ct.khoa) = toString($khoa))
              AND ($he IS NULL OR toLower(trim(coalesce(ct.he, ''))) = toLower(trim($he)))
          }
        WITH n, collect(DISTINCT kw) AS matched_keywords
         WITH n, matched_keywords,
              CASE
              WHEN 'HocPhan' IN labels(n) THEN 3
              WHEN 'KhoiKienThuc' IN labels(n) THEN 2
              WHEN 'Nganh' IN labels(n) THEN 1
              WHEN 'ChuongTrinhDaoTao' IN labels(n) THEN -1
              ELSE 0
              END AS label_bias
        RETURN id(n) AS seed_id,
               labels(n) AS labels,
               properties(n) AS props,
             size(matched_keywords) AS hit_count,
             label_bias AS label_bias
         ORDER BY hit_count DESC, label_bias DESC, seed_id ASC
        LIMIT $seed_limit
        """
        params = {
            "keywords": keywords,
            "search_props": [key for key in _SEARCHABLE_PROP_KEYS if not _is_embedding_key(key)],
            "khoa": filters.get("khoa"),
            "he": filters.get("he"),
            "seed_limit": self.seed_limit,
        }
        driver = get_driver()
        with driver.session() as session:
            return session.run(cypher, **params).data()

    def _prioritize_seed_ids(self, seed_rows: List[Dict[str, Any]], anchor_node_types: List[str]) -> List[int]:
        anchored: List[int] = []
        non_ctdt: List[int] = []
        ctdt: List[int] = []
        seen = set()
        for row in seed_rows:
            seed_id = row.get("seed_id")
            if seed_id is None:
                continue
            seed_id = int(seed_id)
            if seed_id in seen:
                continue
            seen.add(seed_id)
            labels = row.get("labels") or []
            if anchor_node_types and any(anchor in labels for anchor in anchor_node_types):
                anchored.append(seed_id)
            elif "ChuongTrinhDaoTao" in labels:
                ctdt.append(seed_id)
            else:
                non_ctdt.append(seed_id)
        ordered = anchored + non_ctdt + ctdt
        return ordered[: self.seed_limit]

    def _expand_subgraph(
        self,
        seed_ids: List[int],
        filters: Dict[str, Any],
        relation_hints: List[str],
        expansion_policy: Dict[str, int],
    ) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        rows: List[Dict[str, Any]] = []
        # Relation-focused expansion first (e.g., prerequisite/synchronous course), then generic fallback.
        if relation_hints:
            rows = self._run_expand_query(seed_ids, filters, relation_hints=relation_hints, expansion_policy=expansion_policy)
        if not rows:
            rows = self._run_expand_query(seed_ids, filters, relation_hints=[], expansion_policy=expansion_policy)

        return self._collect_rows(rows, seed_ids, expansion_policy)

    def _run_expand_query(
        self,
        seed_ids: List[int],
        filters: Dict[str, Any],
        relation_hints: List[str],
        expansion_policy: Dict[str, int],
    ) -> List[Dict[str, Any]]:
        max_depth = expansion_policy["max_depth"]
        relation_clause = ""
        if relation_hints:
            relation_clause = "AND ANY(rel IN relationships(path) WHERE type(rel) IN $relation_hints)"

        cypher = f"""
        MATCH (n)
        WHERE id(n) IN $seed_ids
        OPTIONAL MATCH path = (n)-[*1..{max_depth}]-(m)
        WHERE path IS NULL OR (
            EXISTS {{
                MATCH (ct:ChuongTrinhDaoTao)-[*0..2]-(m)
                WHERE ($khoa IS NULL OR toString(ct.khoa) = toString($khoa))
                  AND ($he IS NULL OR toLower(trim(coalesce(ct.he, ''))) = toLower(trim($he)))
            }}
            {relation_clause}
        )
        RETURN
            CASE
                WHEN path IS NULL THEN []
                ELSE [x IN nodes(path) | {{id: id(x), labels: labels(x), props: properties(x)}}]
            END AS path_nodes,
            CASE
                WHEN path IS NULL THEN []
                ELSE [r IN relationships(path) | {{
                    id: id(r),
                    type: type(r),
                    start: id(startNode(r)),
                    end: id(endNode(r))
                }}]
            END AS path_relationships
        LIMIT $path_limit
        """
        driver = get_driver()
        with driver.session() as session:
            return session.run(
                cypher,
                seed_ids=seed_ids,
                path_limit=expansion_policy["max_paths"],
                khoa=filters.get("khoa"),
                he=filters.get("he"),
                relation_hints=relation_hints,
            ).data()

    def _collect_rows(
        self,
        rows: List[Dict[str, Any]],
        seed_ids: List[int],
        expansion_policy: Dict[str, int],
    ) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        driver = get_driver()
        nodes_by_id: Dict[int, Dict[str, Any]] = {}
        rels_by_id: Dict[str, Dict[str, Any]] = {}
        max_nodes = min(self.max_nodes, expansion_policy["max_nodes"])

        for row in rows:
            for node in row.get("path_nodes") or []:
                node_id = int(node.get("id"))
                if node_id in nodes_by_id:
                    continue
                nodes_by_id[node_id] = {
                    "id": node_id,
                    "labels": list(node.get("labels", [])),
                    "props": _sanitize_props(node.get("props", {})),
                }
                if len(nodes_by_id) >= max_nodes:
                    break

            for rel in row.get("path_relationships") or []:
                rel_id = str(rel.get("id"))
                if rel_id in rels_by_id:
                    continue
                rels_by_id[rel_id] = {
                    "id": rel_id,
                    "type": str(rel.get("type", "RELATED")),
                    "start": int(rel.get("start")),
                    "end": int(rel.get("end")),
                }
                if len(rels_by_id) >= self.max_relationships:
                    break

            if len(nodes_by_id) >= max_nodes and len(rels_by_id) >= self.max_relationships:
                break

        # Ensure seed nodes are present even when no relationship path exists.
        missing = [sid for sid in seed_ids if sid not in nodes_by_id]
        if missing:
            fill_cypher = """
            MATCH (n)
            WHERE id(n) IN $seed_ids
            RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props
            """
            with driver.session() as session:
                fill_rows = session.run(fill_cypher, seed_ids=missing).data()
            for row in fill_rows:
                node_id = int(row["id"])
                if node_id in nodes_by_id:
                    continue
                nodes_by_id[node_id] = {
                    "id": node_id,
                    "labels": row.get("labels", []),
                    "props": _sanitize_props(row.get("props", {})),
                }

        # Filter dangling relationships if endpoint nodes were dropped by cap.
        valid_ids = set(nodes_by_id.keys())
        rels_by_id = {
            rel_id: rel
            for rel_id, rel in rels_by_id.items()
            if rel["start"] in valid_ids and rel["end"] in valid_ids
        }

        return nodes_by_id, rels_by_id

    def serialize_subgraph(
        self,
        nodes_by_id: Dict[int, Dict[str, Any]],
        rels_by_id: Dict[str, Dict[str, Any]],
    ) -> str:
        if not nodes_by_id:
            return "[GRAPH RESULT]\nKhông có dữ liệu phù hợp."

        node_lines: List[str] = []
        for node in nodes_by_id.values():
            label = node["labels"][0] if node.get("labels") else "Node"
            name = self._pick_name(node.get("props", {}))
            summary_props = self._pick_props(node.get("props", {}))
            if summary_props:
                node_lines.append(f"- {label}: {name} ({summary_props})")
            else:
                node_lines.append(f"- {label}: {name}")

        relation_lines: List[str] = []
        for rel in rels_by_id.values():
            start = nodes_by_id.get(rel["start"], {})
            end = nodes_by_id.get(rel["end"], {})
            start_name = self._pick_name(start.get("props", {}))
            end_name = self._pick_name(end.get("props", {}))
            relation_lines.append(f"- {start_name} -[{rel['type']}]-> {end_name}")

        chunks = ["[GRAPH RESULT]", "Các nút liên quan:"] + node_lines
        if relation_lines:
            chunks += ["", "Các quan hệ chính:"] + relation_lines

        text = "\n".join(chunks).strip()
        if len(text) > self.max_text_chars:
            text = text[: self.max_text_chars] + "\n...<truncated>"
        return text

    def _pick_name(self, props: Dict[str, Any]) -> str:
        for key in _NAME_KEYS:
            value = props.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        for key, value in props.items():
            if _is_embedding_key(key):
                continue
            if isinstance(value, (str, int, float, bool)) and str(value).strip():
                return str(value).strip()
        return "(không rõ)"

    def _pick_props(self, props: Dict[str, Any]) -> str:
        parts: List[str] = []
        seen = set()
        for key in _PROP_KEYS:
            value = props.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if not text:
                continue
            parts.append(f"{key}={text}")
            seen.add(key)
            if len(parts) >= 3:
                return ", ".join(parts)

        for key, value in props.items():
            if key in seen:
                continue
            if _is_embedding_key(key):
                continue
            if isinstance(value, (str, int, float, bool)) and str(value).strip():
                parts.append(f"{key}={value}")
            if len(parts) >= 3:
                break
        return ", ".join(parts)

    def _is_graph_sufficient(
        self,
        nodes_by_id: Dict[int, Dict[str, Any]],
        rels_by_id: Dict[str, Dict[str, Any]],
        graph_text: str,
    ) -> bool:
        if not nodes_by_id:
            return False

        node_count = len(nodes_by_id)
        rel_count = len(rels_by_id)
        text_len = len(graph_text)

        # Enough if graph has at least one meaningful neighborhood or one rich node summary.
        if rel_count >= 1 and node_count >= 2:
            return True
        if node_count >= 1 and text_len >= 180:
            return True
        return False
