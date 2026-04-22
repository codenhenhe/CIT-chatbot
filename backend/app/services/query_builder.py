from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# =========================
# CONSTANTS
# =========================

DEFAULT_KHOA = 51
DEFAULT_HE = "đại trà"

ALLOWED_QUERY_TYPES = {
    "attribute", "relation", "path",
    "aggregation", "description", "list"
}

ALLOWED_RELATIONS = {
    "BAN_HANH_THEO", "DAO_TAO", "THUOC_VE",
    "CAP", "CO", "THAM_KHAO", "DAT_DUOC",
    "YEU_CAU", "GOM", "DOI_VOI",
    "YEU_CAU_TIEN_QUYET", "CO_THE_SONG_HANH"
}

RELATION_TARGET_MAP = {
    "CO": ["ChuanDauRa"],
    "GOM": ["HocPhan", "KhoiKienThuc"],
    "YEU_CAU": ["DieuKienTotNghiep"],
    "BAN_HANH_THEO": ["VanBanPhapLy"],
    "THUOC_VE": ["Nganh"]
}

ENTITY_PROPERTY_MAP = {
    "Nganh": "ten_nganh_vi",
    "HocPhan": "ten_hoc_phan",
    "ChuanDauRa": "noi_dung",
    "DieuKienTotNghiep": "noi_dung",
    "VanBanPhapLy": "ten",
    "KhoiKienThuc": "ten_khoi",
}

# =========================
# RESULT OBJECT (KEEP COMPAT)
# =========================

@dataclass
class QueryBuildResult:
    cypher: str
    params: Dict[str, Any]
    query_type: str
    entity_label: str
    relation: str
    used_defaults: bool
    vector_targets: List[str]


# =========================
# BUILDER
# =========================

class CTDTQueryBuilder:

    def build(self, analysis: Dict[str, Any]) -> QueryBuildResult:
        query_type = self._safe_query_type(analysis.get("query_type"))
        entities = self._parse_entities(analysis.get("entities", []))
        relations = self._parse_relations(analysis.get("relations", []))
        filters = analysis.get("ctdt_filters", {}) or {}
        constraints = analysis.get("constraints", {}) or {}
        vector_targets = analysis.get("vector_targets", []) or []

        # =========================
        # CONTEXT
        # =========================
        params = {
            "nganh": entities.get("Nganh"),
            "khoa": filters.get("khoa"),
            "he": filters.get("he"),
            "limit": constraints.get("limit", 10),
            "depth": constraints.get("depth", 2),
            "entity_value": self._pick_entity_value(entities)
        }

        used_defaults = False

        if query_type != "list":
            if params["khoa"] is None:
                params["khoa"] = DEFAULT_KHOA
                used_defaults = True
            if not params["he"]:
                params["he"] = DEFAULT_HE
                used_defaults = True
        else:
            # list → không default
            params.pop("khoa", None)
            params.pop("he", None)

        relation = self._pick_relation(relations)
        entity_label = self._infer_target_label(relation)

        # Truy vấn tổng tín chỉ toàn CTDT cần đi thẳng vào ct.tong_tin_chi.
        if self._is_ctdt_total_tin_chi_query(analysis, entities):
            cypher = self._build_ctdt_total_tin_chi_query()
            query_type = "attribute"
            relation = "THUOC_VE"
            entity_label = "ChuongTrinhDaoTao"
            vector_targets = []
            return QueryBuildResult(
                cypher=cypher,
                params=params,
                query_type=query_type,
                entity_label=entity_label,
                relation=relation,
                used_defaults=used_defaults,
                vector_targets=vector_targets,
            )

        # =========================
        # BUILD CYPHER
        # =========================

        if query_type == "list":
            cypher = self._build_list_query(params)

        elif query_type == "path":
            cypher = self._build_path_query(relation, params)

        elif query_type == "aggregation":
            cypher = self._build_aggregation_query(relation, entity_label)

        elif query_type == "attribute":
            cypher = self._build_attribute_query(relation, entity_label)

        elif query_type == "relation":
            cypher = self._build_relation_query(relation, entity_label)

        else:  # description fallback
            cypher = self._build_relation_query(relation, entity_label)

        return QueryBuildResult(
            cypher=cypher,
            params=params,
            query_type=query_type,
            entity_label=entity_label,
            relation=relation,
            used_defaults=used_defaults,
            vector_targets=vector_targets
        )

    # =========================
    # NORMALIZATION
    # =========================

    def _safe_query_type(self, qt: str) -> str:
        qt = (qt or "").lower()
        return qt if qt in ALLOWED_QUERY_TYPES else "description"

    def _parse_entities(self, entities_raw) -> Dict[str, str]:
        result = {}
        for e in entities_raw:
            if isinstance(e, dict):
                t = e.get("type")
                v = e.get("value")
                if t and v:
                    result[t] = v
        return result

    def _parse_relations(self, relations_raw) -> List[str]:
        return [r for r in relations_raw if r in ALLOWED_RELATIONS]

    def _pick_relation(self, relations: List[str]) -> str:
        return relations[0] if relations else "CO"  # fallback safe

    def _infer_target_label(self, relation: str) -> str:
        return RELATION_TARGET_MAP.get(relation, ["HocPhan"])[0]

    def _pick_entity_value(self, entities: Dict[str, str]) -> Optional[str]:
        return next(iter(entities.values()), None)

    def _is_ctdt_total_tin_chi_query(self, analysis: Dict[str, Any], entities: Dict[str, str]) -> bool:
        query_type = self._safe_query_type(analysis.get("query_type"))
        rewrite = str(analysis.get("rewrite", "")).strip().lower()
        has_nganh = bool(entities.get("Nganh"))

        total_markers = ("tong tin chi", "tổng tín chỉ", "tong so tin chi", "tổng số tín chỉ")
        completion_markers = ("hoan thanh", "hoàn thành", "tot nghiep", "tốt nghiệp")
        program_markers = ("chuong trinh", "chương trình", "ctdt")

        has_total = any(marker in rewrite for marker in total_markers)
        has_completion = any(marker in rewrite for marker in completion_markers)
        has_program = any(marker in rewrite for marker in program_markers)

        # Ưu tiên bắt theo ý nghĩa câu hỏi thay vì relation do LLM đoán.
        return has_nganh and has_total and has_program and (has_completion or query_type in {"attribute", "aggregation", "description"})

    # =========================
    # CORE MATCH
    # =========================

    def _ctdt_match(self) -> str:
        return """
        MATCH (ct:ChuongTrinhDaoTao)
        OPTIONAL MATCH (ct)-[:THUOC_VE]->(n:Nganh)
                WHERE (
                        $nganh IS NULL
                        OR toLower(trim(coalesce(n.ten_nganh_vi, ""))) = toLower(trim($nganh))
                )
                    AND (
                        $khoa IS NULL
                        OR toString(ct.khoa) = toString($khoa)
                    )
                    AND (
                        $he IS NULL
                        OR toLower(trim(coalesce(ct.he, ""))) = toLower(trim($he))
                    )
        WITH ct
        """

    # =========================
    # QUERY TYPES
    # =========================

    def _build_list_query(self, params) -> str:
        return """
        MATCH (n:Nganh {ten_nganh_vi: $nganh})
        MATCH (ct:ChuongTrinhDaoTao)-[:THUOC_VE]->(n)
        RETURN ct
        ORDER BY ct.khoa DESC
        LIMIT $limit
        """

    def _build_relation_query(self, relation, label) -> str:
        return self._ctdt_match() + f"""
        MATCH (ct)-[:{relation}]->(n:{label})
        RETURN n
        """

    def _build_attribute_query(self, relation, label) -> str:
        prop = ENTITY_PROPERTY_MAP.get(label, "ten")
        return self._ctdt_match() + f"""
        MATCH (ct)-[:{relation}]->(n:{label})
        RETURN n.{prop} AS value
        """

    def _build_aggregation_query(self, relation, label) -> str:
        if relation == "GOM" and label == "HocPhan":
            return self._ctdt_match() + """
            MATCH (ct)-[:GOM]->(n:HocPhan)
            RETURN sum(coalesce(toInteger(n.so_tin_chi), 0)) AS total_tin_chi
            """
        return self._ctdt_match() + f"""
        MATCH (ct)-[:{relation}]->(n:{label})
        RETURN count(n) AS total
        """

    def _build_ctdt_total_tin_chi_query(self) -> str:
        return """
        MATCH (ct:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
                WHERE (
                        $nganh IS NULL
                        OR toLower(trim(coalesce(n.ten_nganh_vi, ""))) = toLower(trim($nganh))
                )
                    AND (
                        $khoa IS NULL
                        OR toString(ct.khoa) = toString($khoa)
                    )
                    AND (
                        $he IS NULL
                        OR toLower(trim(coalesce(ct.he, ""))) = toLower(trim($he))
                    )
        RETURN n.ten_nganh_vi AS nganh,
               ct.khoa AS khoa,
               ct.he AS he,
               ct.tong_tin_chi AS total_tin_chi
        ORDER BY ct.khoa DESC
        LIMIT 1
        """

    def _build_path_query(self, relation, params) -> str:
        depth = int(params.get("depth", 2))
        return self._ctdt_match() + f"""
        MATCH path = (ct)-[:{relation}*1..{depth}]->(n)
        RETURN path
        """