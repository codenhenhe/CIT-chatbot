import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    from app.scripts.term_dictionary import normalize_token, slugify_token
else:
    from .term_dictionary import normalize_token, slugify_token


NODE_PROPERTIES: Dict[str, Tuple[str, ...]] = {
    "ChuongTrinhDaoTao": (
        "ma_chuong_trinh",
        "khoa",
        "he",
        "ngon_ngu",
        "tong_tin_chi",
        "thoi_gian_dao_tao",
        "thang_diem",
    ),
    "DoiTuongTuyenSinh": ("noi_dung",),
    "DieuKienTotNghiep": ("noi_dung",),
    "HocPhan": (
        "ma_hoc_phan",
        "ten_hoc_phan",
        "so_tin_chi",
        "so_tiet_ly_thuyet",
        "so_tiet_thuc_hanh",
        "tom_tat",
        "bat_buoc",
    ),
    "KhoiKienThuc": (
        "ma_khoi",
        "ten_khoi",
        "tong_tin_chi",
        "tin_chi_bat_buoc",
        "tin_chi_tu_chon",
    ),
    "ChuanDauRa": ("ma_chuan", "noi_dung", "nhom", "loai"),
    "VanBanPhapLy": ("so", "ten", "loai", "ngay_ban_hanh", "co_quan_ban_hanh", "noi_dung_goc"),
    "Nganh": ("ma_nganh", "ten_nganh_vi", "ten_nganh_en"),
    "Khoa": ("ma_khoa", "ten_khoa"),
    "BoMon": ("ma_bo_mon", "ten_bo_mon"),
    "TrinhDo": ("ma_trinh_do", "ten_trinh_do"),
    "LoaiVanBang": ("ma_loai", "loai_van_bang"),
    "HinhThucDaoTao": ("ma_hinh_thuc", "ten_hinh_thuc"),
    "PhuongThucDaoTao": ("ma_phuong_thuc", "ten_phuong_thuc"),
    "MucTieuDaoTao": ("loai", "noi_dung"),
    "ViTriViecLam": ("noi_dung",),
    "ChuanThamKhao": ("noi_dung", "link", "noi_dung_goc"),
    "DanhGiaKiemDinh": ("noi_dung",),
    "KhaNangHocTap": ("noi_dung",),
    "NhomHocPhanTuChon": ("ten_nhom",),
    "YeuCauTuChon": ("noi_dung_yeu_cau", "so_tin_chi_yeu_cau"),
}

LEGAL_DOC_RE = re.compile(
    r"(Quyết định|Thông tư|Nghị quyết)\s+số\s+([0-9]+(?:/[\wĐ-]+)+)\s+ngày\s+([0-9]{1,2}\s+tháng\s+[0-9]{1,2}\s+năm\s+[0-9]{4})",
    re.IGNORECASE,
)
PLO_RE = re.compile(r"\((PLO\d{1,3})\)", re.IGNORECASE)
CREDIT_RE = re.compile(r"(\d{1,3})\s*(?:tín chỉ|TC)", re.IGNORECASE)
COURSE_CODE_RE = re.compile(r"\b[A-Z]{1,5}\s*\d{2,}[A-Z0-9]*\b")
BULLET_RE = re.compile(r"^(?:[-*]|[a-z]\.|[a-z]\)|\d+\.)\s*(.+)$", re.IGNORECASE)

KHOA_CODE_RULES: Dict[str, str] = {
    "mang may tinh va truyen thong": "MMT",
    "khoa hoc may tinh": "KHMT",
    "ky thuat phan mem": "KTPM",
    "he thong thong tin": "HTTT",
    "cong nghe thong tin": "CNTT",
    "cong nghe phan mem": "CNPM",
    "cong nghe phan men": "CNPM",
    "an toan thong tin": "ATTT",
    "truyen thong da phuong tien": "TTDPT",
    "tri tue nhan tao": "TTNT",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize(value: str) -> str:
    return normalize_token(value or "")


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _parse_int(value: Any) -> Optional[int]:
    text = _clean_text(value)
    m = re.search(r"-?\d+", text)
    return int(m.group(0)) if m else None


def _parse_float(value: Any) -> Optional[float]:
    text = _clean_text(value).replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(m.group(0)) if m else None


def _compact(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _strip_heading_prefix(text: str) -> str:
    cleaned = _clean_text(text)
    cleaned = re.sub(r"^[-*]\s+", "", cleaned)
    cleaned = re.sub(r"^\d+(?:\.\d+)*\.?\s*", "", cleaned)
    cleaned = re.sub(r"^[a-zA-Z][.)]\s*", "", cleaned)
    return _compact(cleaned)


def _split_compound_content(text: str, split_sentences: bool = False) -> List[str]:
    normalized = _clean_text(text)
    if not normalized:
        return []

    normalized = normalized.replace("\n", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()

    parts: List[str] = []
    bullet_parts = [p.strip() for p in re.split(r"(?:^|[\.;\n])\s*-\s+", normalized) if p.strip()]
    if len(bullet_parts) > 1:
        parts.extend(bullet_parts)
    else:
        enum_parts = [p.strip() for p in re.split(r"\s*(?:\d+\.|[a-zA-Z]\))\s+", normalized) if p.strip()]
        if len(enum_parts) > 1:
            parts.extend(enum_parts)
        else:
            parts.append(normalized)

    refined: List[str] = []
    for part in parts:
        chunk = _strip_heading_prefix(part).strip(" ;")
        if not chunk:
            continue
        if split_sentences and len(chunk) > 220:
            sent_parts = [s.strip() for s in re.split(r"(?<=[\.;:])\s+(?=[A-ZÀ-Ỵ])", chunk) if s.strip()]
            if len(sent_parts) > 1:
                refined.extend(s.strip(" ;") for s in sent_parts if len(_strip_heading_prefix(s)) >= 15)
                continue
        refined.append(chunk)

    deduped: List[str] = []
    seen = set()
    for part in refined:
        signature = _normalize(part)
        if not signature or signature in seen or len(part) < 12:
            continue
        seen.add(signature)
        deduped.append(part)
    return deduped


def _resolve_khoa_code(khoa_name: str) -> str:
    norm_name = _normalize(khoa_name)
    for rule_name, code in KHOA_CODE_RULES.items():
        if rule_name in norm_name:
            return code
    return slugify_token(khoa_name)


class GraphState:
    def __init__(self) -> None:
        self.results: Dict[str, Dict[str, Any]] = {
            node_type: {"node_type": node_type, "items": []} for node_type in NODE_PROPERTIES
        }
        self.relationships: List[Dict[str, str]] = []
        self._node_signatures: Dict[str, set] = {node_type: set() for node_type in NODE_PROPERTIES}
        self._rel_signatures: set = set()

        self.ctdt_match: str = "ChuongTrinhDaoTao"
        self.nganh_match: Optional[str] = None
        self.khoa_match: Optional[str] = None
        self.program_id: Optional[str] = None

        self.info_map: Dict[str, str] = {}
        self.flat_sections: List[Dict[str, Any]] = []
        self.full_text: str = ""

        self.khoi_by_norm: Dict[str, str] = {}
        self.hoc_phan_by_code: Dict[str, str] = {}

    def add_node(self, node_type: str, values: Dict[str, Any]) -> Dict[str, Any]:
        props = NODE_PROPERTIES[node_type]
        payload = {prop: values.get(prop) for prop in props}
        signature = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        if signature in self._node_signatures[node_type]:
            return payload
        self._node_signatures[node_type].add(signature)
        self.results[node_type]["items"].append(payload)
        return payload

    def add_rel(
        self,
        source_node_type: str,
        source_match: str,
        rel_type: str,
        target_node_type: str,
        target_match: str,
        evidence: str,
    ) -> None:
        payload = {
            "source_node_type": source_node_type,
            "source_match": _compact(source_match),
            "rel_type": rel_type,
            "target_node_type": target_node_type,
            "target_match": _compact(target_match),
            "evidence": _compact(evidence),
        }
        signature = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if signature in self._rel_signatures:
            return
        self._rel_signatures.add(signature)
        self.relationships.append(payload)


def _flatten_sections(sections: Any, path: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    path = path or []
    out: List[Dict[str, Any]] = []
    for sec in _as_list(sections):
        if not isinstance(sec, dict):
            continue
        title = _clean_text(sec.get("title", ""))
        next_path = [*path]
        if title:
            next_path.append(title)
        out.append(
            {
                "heading": _clean_text(sec.get("heading", "")),
                "title": title,
                "path": next_path,
                "path_text": " / ".join(next_path),
                "text_content": _clean_text(sec.get("text_content", "")),
                "tables": _as_list(sec.get("tables")),
                "source": sec,
            }
        )
        out.extend(_flatten_sections(sec.get("children"), next_path))
    return out


def _find_section(state: GraphState, keyword: str) -> Optional[Dict[str, Any]]:
    key = _normalize(keyword)
    for sec in state.flat_sections:
        if key in _normalize(sec.get("title", "")):
            return sec
    return None


def _section_text_recursive(section: Dict[str, Any]) -> str:
    src = section.get("source") or {}
    chunks = []
    text = _clean_text(src.get("text_content", ""))
    if text:
        chunks.append(text)
    for child in _as_list(src.get("children")):
        chunks.append(_section_text_recursive({"source": child}))
    return "\n".join(chunk for chunk in chunks if chunk).strip()


def _table_rows(table: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = table.get("records") or []
    return [r for r in records if isinstance(r, dict)]


def _table_columns(table: Dict[str, Any]) -> List[str]:
    cols = []
    for col in table.get("columns") or []:
        cols.append(_clean_text(col))
    return cols


def _row_value_by_label(table: Dict[str, Any], row: Dict[str, Any], contains: str) -> str:
    cols = _table_columns(table)
    target = _normalize(contains)
    for idx, label in enumerate(cols, start=1):
        if target in _normalize(label):
            return _clean_text(row.get(f"col_{idx}", ""))
    return ""


def _build_info_map(state: GraphState) -> None:
    sec = _find_section(state, "Thông tin chung về chương trình đào tạo")
    if not sec:
        return

    for table in sec.get("tables", []):
        for row in _table_rows(table):
            key = _clean_text(row.get("col_1", ""))
            val = _clean_text(row.get("col_2", ""))
            if key and val:
                state.info_map[_normalize(key)] = val


def extract_node_chuong_trinh_dao_tao(state: GraphState, source: Optional[str]) -> None:
    ma_nganh = None
    ten_vi = None
    tong_tc = None
    thoi_gian = None
    thang_diem = None

    for key, value in state.info_map.items():
        if "ma so nganh" in key:
            m = re.search(r"\d{4,10}", _clean_text(value))
            ma_nganh = m.group(0) if m else None
        elif "ten chuong trinh tieng viet" in key:
            ten_vi = value
        elif "so tin chi" in key:
            tong_tc = _parse_int(value)
        elif "thoi gian dao tao" in key:
            thoi_gian = _parse_float(value)
        elif "thang diem" in key:
            thang_diem = _parse_float(value)

    # Determine he: default Dai tra unless chat luong cao appears.
    full_text_norm = _normalize(state.full_text)
    he = "Chất lượng cao" if ("chat luong cao" in full_text_norm or re.search(r"\bclc\b", full_text_norm)) else "Đại trà"
    he_code = "CLC" if he == "Chất lượng cao" else "STD"

    # Determine khoa from source / cover / info / fallback year.
    cohort = None
    if source:
        m = re.search(r"k([0-9]{2})", source, re.IGNORECASE)
        if m:
            cohort = int(m.group(1))

    if cohort is None:
        m = re.search(r"\bkhoa\s*([0-9]{2})\b", full_text_norm)
        if m:
            cohort = int(m.group(1))

    if not ma_nganh:
        m = re.search(r"\b(\d{4,10})\b", state.full_text)
        ma_nganh = m.group(1) if m else None

    if cohort is None:
        year = None
        legal_match = LEGAL_DOC_RE.search(state.full_text)
        if legal_match:
            year_match = re.search(r"\b(19\d{2}|20\d{2})\b", legal_match.group(3))
            if year_match:
                year = int(year_match.group(1))
        if year is None:
            year_match = re.search(r"\b(19\d{2}|20\d{2})\b", state.full_text)
            if year_match:
                year = int(year_match.group(1))
        if year is not None:
            cohort = year - 1974

    program_id = None
    if ma_nganh and cohort:
        program_id = f"{ma_nganh}_{cohort}_{he_code}"
    elif ma_nganh:
        program_id = f"{ma_nganh}_UNKNOWN_{he_code}"

    item = state.add_node(
        "ChuongTrinhDaoTao",
        {
            "ma_chuong_trinh": program_id,
            "khoa": cohort,
            "he": he,
            "ngon_ngu": "Tiếng Việt",
            "tong_tin_chi": tong_tc,
            "thoi_gian_dao_tao": thoi_gian,
            "thang_diem": thang_diem,
        },
    )
    state.program_id = item.get("ma_chuong_trinh")
    state.ctdt_match = state.program_id or ten_vi or "ChuongTrinhDaoTao"


def extract_node_van_ban_phap_ly(state: GraphState) -> None:
    match = LEGAL_DOC_RE.search(state.full_text)
    if not match:
        return

    loai, so, ngay = match.groups()
    ten = f"{loai.title()} số {so}"
    item = state.add_node(
        "VanBanPhapLy",
        {
            "so": so,
            "ten": ten,
            "loai": loai.title(),
            "ngay_ban_hanh": ngay,
            "co_quan_ban_hanh": "Trường Đại học Cần Thơ" if "Trường Đại học Cần Thơ" in state.full_text else None,
            "noi_dung_goc": match.group(0),
        },
    )
    state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "BAN_HANH_THEO", "VanBanPhapLy", item["ten"], match.group(0))


def extract_node_nganh(state: GraphState) -> None:
    ten_vi = None
    ten_en = None
    ma_nganh = None

    for key, value in state.info_map.items():
        if "ten chuong trinh tieng viet" in key:
            ten_vi = value
        elif "ten chuong trinh tieng anh" in key:
            ten_en = value
        elif "ma so nganh" in key:
            ma_nganh = _parse_int(value)

    item = state.add_node(
        "Nganh",
        {
            "ma_nganh": ma_nganh,
            "ten_nganh_vi": ten_vi,
            "ten_nganh_en": ten_en,
        },
    )
    state.nganh_match = item.get("ten_nganh_vi") or str(item.get("ma_nganh") or "Nganh")
    state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "THUOC_VE", "Nganh", state.nganh_match, state.nganh_match)


def extract_node_khoa(state: GraphState) -> None:
    management_unit = None
    khoa_from_info = None
    for key, value in state.info_map.items():
        if "don vi quan ly" in key:
            management_unit = value
        elif key.startswith("khoa"):
            khoa_from_info = value
    if not management_unit:
        cover_section = _find_section(state, "Trang bìa")
        cover_text = _section_text_recursive(cover_section) if cover_section else state.full_text
        for line in cover_text.splitlines():
            line_clean = _clean_text(line)
            if "don vi quan ly" not in _normalize(line_clean):
                continue
            if ":" in line_clean:
                management_unit = _clean_text(line_clean.split(":", 1)[1])
            else:
                management_unit = line_clean
            break

    khoa_name = _clean_text(khoa_from_info) if khoa_from_info else None
    if management_unit:
        parts = [part.strip() for part in re.split(r"[,;]", management_unit) if part.strip()]
        khoa_name = next((part for part in parts if "khoa" in _normalize(part)), None) or parts[0]
    if not khoa_name:
        cover_section = _find_section(state, "Trang bìa")
        cover_text = _section_text_recursive(cover_section) if cover_section else state.full_text
        for line in cover_text.splitlines():
            line_clean = _clean_text(line)
            norm = _normalize(line_clean)
            if norm.startswith("khoa "):
                khoa_name = line_clean
                break
    if khoa_name:
        ma_khoa = _resolve_khoa_code(khoa_name)
        khoa = state.add_node("Khoa", {"ma_khoa": ma_khoa, "ten_khoa": khoa_name})
        state.khoa_match = khoa["ten_khoa"]
        if state.nganh_match:
            evidence = management_unit or khoa_from_info or khoa_name
            state.add_rel("Nganh", state.nganh_match, "THUOC_VE", "Khoa", khoa["ten_khoa"], evidence)


def extract_node_bo_mon(state: GraphState) -> None:
    bo_mon_name = None
    for key, value in state.info_map.items():
        if "bo mon" in key:
            bo_mon_name = _clean_text(value)
            break

    if not bo_mon_name:
        cover_section = _find_section(state, "Trang bìa")
        cover_text = _section_text_recursive(cover_section) if cover_section else state.full_text
        for line in cover_text.splitlines():
            line_clean = _clean_text(line)
            if _normalize(line_clean).startswith("bo mon"):
                bo_mon_name = line_clean
                break

    if not bo_mon_name:
        state.add_node("BoMon", {"ma_bo_mon": None, "ten_bo_mon": None})
        return

    bo_mon = state.add_node(
        "BoMon",
        {"ma_bo_mon": slugify_token(bo_mon_name), "ten_bo_mon": bo_mon_name},
    )

    if state.khoa_match:
        state.add_rel("BoMon", bo_mon["ten_bo_mon"], "THUOC_VE", "Khoa", state.khoa_match, bo_mon_name)
    if state.nganh_match:
        state.add_rel("Nganh", state.nganh_match, "THUOC_VE", "BoMon", bo_mon["ten_bo_mon"], bo_mon_name)


def extract_node_trinh_do(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "trinh do dao tao" in key:
            value = val
            break
    if not value:
        return

    item = state.add_node("TrinhDo", {"ma_trinh_do": slugify_token(value), "ten_trinh_do": value})
    state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "DAO_TAO", "TrinhDo", item["ten_trinh_do"], value)


def extract_node_loai_van_bang(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "ten goi van bang" in key:
            value = val
            break
    if not value:
        return

    loai = value
    if "kỹ sư" in value.lower():
        loai = "Kỹ sư"
    elif "cử nhân" in value.lower():
        loai = "Cử nhân"

    item = state.add_node("LoaiVanBang", {"ma_loai": slugify_token(loai), "loai_van_bang": loai})
    state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CAP", "LoaiVanBang", item["loai_van_bang"], value)


def extract_node_hinh_thuc_dao_tao(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "hinh thuc dao tao" in key:
            value = val
            break
    if not value:
        return

    for token in re.split(r"[,;/]|\s+và\s+", value):
        token = _clean_text(token)
        if not token:
            continue
        item = state.add_node(
            "HinhThucDaoTao",
            {"ma_hinh_thuc": slugify_token(token), "ten_hinh_thuc": token},
        )
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "HinhThucDaoTao", item["ten_hinh_thuc"], value)


def extract_node_phuong_thuc_dao_tao(state: GraphState) -> None:
    sec = _find_section(state, "Phương pháp giảng dạy và học tập")
    text = _section_text_recursive(sec) if sec else state.full_text
    text_norm = _normalize(text)
    candidates = []
    if "truc tiep" in text_norm:
        candidates.append("Trực tiếp")
    if "truc tuyen" in text_norm:
        candidates.append("Trực tuyến")
    
    if not candidates:
        candidates.append("Trực tiếp")

    for value in candidates:
        item = state.add_node(
            "PhuongThucDaoTao",
            {"ma_phuong_thuc": slugify_token(value), "ten_phuong_thuc": value},
        )
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "PhuongThucDaoTao", item["ten_phuong_thuc"], text[:200])


def extract_node_doi_tuong_tuyen_sinh(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "doi tuong tuyen sinh" in key:
            value = val
            break
    if not value:
        sec = _find_section(state, "Tiêu chí tuyển sinh")
        value = _section_text_recursive(sec) if sec else None
    if not value:
        return

    parts = _split_compound_content(value)
    if not parts:
        parts = [_clean_text(value)]

    for part in parts:
        item = state.add_node("DoiTuongTuyenSinh", {"noi_dung": part})
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "DoiTuongTuyenSinh", item["noi_dung"], part[:200])


def extract_node_dieu_kien_tot_nghiep(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "dieu kien tot nghiep" in key:
            value = val
            break
    if not value:
        m = re.search(r"(Hoàn thành|Tích lũy).{30,300}", state.full_text, re.IGNORECASE)
        value = _clean_text(m.group(0)) if m else None
    if not value:
        return
    parts = _split_compound_content(value)
    if not parts:
        parts = [_clean_text(value)]

    for part in parts:
        item = state.add_node("DieuKienTotNghiep", {"noi_dung": part})
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "YEU_CAU", "DieuKienTotNghiep", item["noi_dung"], part[:200])


def extract_node_muc_tieu_dao_tao(state: GraphState) -> None:
    sec = _find_section(state, "Mục tiêu đào tạo")
    if not sec:
        return

    children = _as_list((sec.get("source") or {}).get("children")) or [sec.get("source")]

    def collect_objectives(node: Dict[str, Any]) -> List[str]:
        found: List[str] = []
        title = _strip_heading_prefix(_clean_text(node.get("title", "")))
        text = _clean_text(node.get("text_content", ""))
        children_nodes = _as_list(node.get("children"))

        if children_nodes:
            for child in children_nodes:
                found.extend(collect_objectives(child))
        else:
            paragraph = _strip_heading_prefix(text)
            title_norm = _normalize(title)
            paragraph_norm = _normalize(paragraph)

            if title and paragraph:
                if paragraph_norm.startswith(title_norm) and len(paragraph) >= len(title) + 10:
                    found.append(paragraph)
                elif title_norm.startswith(paragraph_norm) and len(title) >= len(paragraph):
                    found.append(title)
                elif title_norm in paragraph_norm or paragraph_norm in title_norm:
                    found.append(paragraph if len(paragraph) >= len(title) else title)
                else:
                    if len(title) >= 12:
                        found.append(title)
                    if len(paragraph) >= 20:
                        found.append(paragraph)
            elif title and len(title) >= 12:
                found.append(title)
            elif len(paragraph) >= 20:
                found.append(paragraph)
        return found

    for child in children:
        title = _clean_text((child or {}).get("title", ""))
        loai = "cu_the" if "cu the" in _normalize(title) else "chung"
        objectives = collect_objectives(child or {})

        if not objectives:
            fallback = _strip_heading_prefix(_clean_text((child or {}).get("text_content", "")))
            if len(fallback) >= 20:
                objectives = [fallback]

        for obj in objectives:
            item = state.add_node("MucTieuDaoTao", {"loai": loai, "noi_dung": obj})
            state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "MucTieuDaoTao", item["noi_dung"], obj[:200])


def extract_node_chuan_dau_ra(state: GraphState) -> None:
    sec = _find_section(state, "Chuẩn đ")
    if not sec:
        return

    root = sec.get("source") or {}

    def visit(node: Dict[str, Any], top_group: Optional[str], sub_group: Optional[str]) -> None:
        heading = _clean_text(node.get("heading", ""))
        title = _strip_heading_prefix(_clean_text(node.get("title", "")))
        text = _clean_text(node.get("text_content", ""))
        children = _as_list(node.get("children"))

        current_top = top_group
        current_sub = sub_group
        if re.match(r"^3\.\d+\.$", heading) and title:
            current_top = title
            current_sub = title
        elif re.match(r"^3\.\d+\.\d+\.$", heading) and title:
            current_sub = title

        if children:
            for child in children:
                visit(child, current_top, current_sub)
            return

        candidates: List[str] = []
        paragraph = _strip_heading_prefix(text)
        title_norm = _normalize(title)
        paragraph_norm = _normalize(paragraph)

        if title and paragraph:
            if paragraph_norm.startswith(title_norm) and len(paragraph) >= len(title) + 10:
                candidates.append(paragraph)
            elif title_norm.startswith(paragraph_norm) and len(title) >= len(paragraph):
                candidates.append(title)
            elif title_norm in paragraph_norm or paragraph_norm in title_norm:
                candidates.append(paragraph if len(paragraph) >= len(title) else title)
            else:
                candidates.append(title)
                if len(paragraph) >= 15:
                    candidates.append(paragraph)
        elif title:
            candidates.append(title)
        elif len(paragraph) >= 15:
            candidates.append(paragraph)

        for candidate in candidates:
            ma_chuan = None
            code = PLO_RE.search(candidate)
            noi_dung = candidate
            if code:
                ma_chuan = code.group(1).upper()
                noi_dung = PLO_RE.sub("", noi_dung).strip(" .;")

            if len(noi_dung) < 12:
                continue

            item = state.add_node(
                "ChuanDauRa",
                {
                    "ma_chuan": ma_chuan,
                    "noi_dung": noi_dung,
                    "nhom": current_top or "Chuẩn đầu ra",
                    "loai": current_sub or current_top or "Chuẩn đầu ra",
                },
            )
            state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "ChuanDauRa", item["noi_dung"], noi_dung[:200])

    for child in _as_list(root.get("children")):
        visit(child, None, None)


def extract_node_vi_tri_viec_lam(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "vi tri viec lam" in key:
            value = val
            break
    if not value:
        sec = _find_section(state, "Vị trí việc làm")
        value = _section_text_recursive(sec) if sec else None
    if not value:
        return

    parts = _split_compound_content(value)
    if not parts:
        parts = [_clean_text(value)]

    for part in parts:
        item = state.add_node("ViTriViecLam", {"noi_dung": part})
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "ViTriViecLam", item["noi_dung"], part[:200])


def extract_node_chuan_tham_khao(state: GraphState) -> None:
    sec = _find_section(state, "tham khảo")
    if not sec:
        return
    text = _section_text_recursive(sec)
    for line in text.splitlines():
        line = _clean_text(line)
        if not line:
            continue
        if not line.startswith("-") and "http" not in line.lower():
            continue
        url_match = re.search(r"https?://\S+", line)
        item = state.add_node(
            "ChuanThamKhao",
            {"noi_dung": line.lstrip("- "), "link": url_match.group(0) if url_match else None, "noi_dung_goc": line},
        )
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "THAM_KHAO", "ChuanThamKhao", item["noi_dung"], line)


def extract_node_danh_gia_kiem_dinh(state: GraphState) -> None:
    value = None
    for key, val in state.info_map.items():
        if "danh gia kiem dinh" in key:
            value = val
            break

    if not value:
        sec = _find_section(state, "Phương pháp đánh giá") or _find_section(state, "kiểm định")
        if not sec:
            return
        value = _section_text_recursive(sec)
    if not value:
        return

    parts = _split_compound_content(value, split_sentences=True)
    if not parts:
        parts = [_clean_text(value)]

    for part in parts:
        item = state.add_node("DanhGiaKiemDinh", {"noi_dung": part})
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "CO", "DanhGiaKiemDinh", item["noi_dung"], part[:200])


def extract_node_kha_nang_hoc_tap(state: GraphState) -> None:
    sec = _find_section(state, "Khả năng học tập")
    if not sec:
        return
    text = _section_text_recursive(sec)
    if not text:
        return
    item = state.add_node("KhaNangHocTap", {"noi_dung": text})
    state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "DAT_DUOC", "KhaNangHocTap", item["noi_dung"], text[:200])


def extract_node_khoi_kien_thuc(state: GraphState) -> None:
    sec = _find_section(state, "Cấu trúc chương trình dạy học")
    text = _section_text_recursive(sec) if sec else state.full_text

    pattern = re.compile(
        r"Khối kiến thức\s+(.+?)\s*:\s*(\d+)\s*tín chỉ\s*\(Bắt buộc:\s*(\d+)\s*tín chỉ;\s*Tự chọn:\s*(\d+)\s*tín chỉ\)",
        re.IGNORECASE,
    )
    for m in pattern.finditer(text):
        name = _clean_text(m.group(1))
        ten_khoi = f"Khối kiến thức {name}"
        item = state.add_node(
            "KhoiKienThuc",
            {
                "ma_khoi": slugify_token(ten_khoi),
                "ten_khoi": ten_khoi,
                "tong_tin_chi": int(m.group(2)),
                "tin_chi_bat_buoc": int(m.group(3)),
                "tin_chi_tu_chon": int(m.group(4)),
            },
        )
        key = _normalize(item["ten_khoi"])
        state.khoi_by_norm[key] = item["ten_khoi"]
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "GOM", "KhoiKienThuc", item["ten_khoi"], m.group(0))


def _extract_course_rows(state: GraphState) -> List[Tuple[Dict[str, Any], Dict[str, Any], str]]:
    rows: List[Tuple[Dict[str, Any], Dict[str, Any], str]] = []
    for sec in state.flat_sections:
        title_norm = _normalize(sec.get("title", ""))
        if "khung chuong trinh dao tao" not in title_norm and "ke hoach day hoc" not in title_norm and "mo ta tom tat" not in title_norm:
            continue
        for table in sec.get("tables", []):
            for row in _table_rows(table):
                rows.append((table, row, sec.get("title", "")))
    return rows


def extract_node_hoc_phan(state: GraphState) -> None:
    pending_rel: List[Tuple[str, str, str, str]] = []
    current_khoi: Optional[str] = None

    # Group raw rows by course identity first to avoid duplicate HocPhan nodes.
    grouped: Dict[str, Dict[str, Any]] = {}

    for table, row, section_title in _extract_course_rows(state):
        row_text = " | ".join(_clean_text(v) for v in row.values() if _clean_text(v))
        if not row_text:
            continue

        if "khối kiến thức" in _normalize(row_text):
            for khoi_key, khoi_name in state.khoi_by_norm.items():
                if khoi_key in _normalize(row_text):
                    current_khoi = khoi_name
                    break
            continue

        ma_hp = _row_value_by_label(table, row, "Mã số") or _row_value_by_label(table, row, "Mã số HP") or row.get("col_2", "")
        ma_hp = _clean_text(ma_hp).replace(" ", "")
        if ma_hp and not COURSE_CODE_RE.search(ma_hp):
            ma_hp = ""

        ten_hp = _row_value_by_label(table, row, "Tên học") or row.get("col_3", "")
        ten_hp = _clean_text(ten_hp)
        if not ten_hp:
            continue

        so_tc = _parse_int(_row_value_by_label(table, row, "Số tín chỉ") or row.get("col_4", ""))
        so_lt = _parse_int(_row_value_by_label(table, row, "LT") or row.get("col_7", ""))
        so_th = _parse_int(_row_value_by_label(table, row, "TH") or row.get("col_8", ""))
        tom_tat = _row_value_by_label(table, row, "Mô tả tóm tắt")

        bat_buoc = None
        if _parse_int(_row_value_by_label(table, row, "Bắt buộc") or row.get("col_5", "")):
            bat_buoc = True
        elif _parse_int(_row_value_by_label(table, row, "Tự chọn") or row.get("col_6", "")):
            bat_buoc = False

        if ma_hp:
            group_key = f"CODE::{_normalize(ma_hp)}"
        else:
            group_key = f"NAME::{_normalize(ten_hp)}"

        group = grouped.setdefault(
            group_key,
            {
                "ma_hp": ma_hp or None,
                "name_counts": {},
                "so_tc": {},
                "so_lt": {},
                "so_th": {},
                "tom_tat": {},
                "bat_buoc": {},
                "sections": set(),
                "khoi": set(),
                "elective_groups": set(),
                "raw_rel": [],
            },
        )

        group["name_counts"][ten_hp] = group["name_counts"].get(ten_hp, 0) + 1
        if so_tc is not None:
            group["so_tc"][so_tc] = group["so_tc"].get(so_tc, 0) + 1
        if so_lt is not None:
            group["so_lt"][so_lt] = group["so_lt"].get(so_lt, 0) + 1
        if so_th is not None:
            group["so_th"][so_th] = group["so_th"].get(so_th, 0) + 1
        if tom_tat:
            group["tom_tat"][tom_tat] = group["tom_tat"].get(tom_tat, 0) + 1
        if bat_buoc is not None:
            group["bat_buoc"][bat_buoc] = group["bat_buoc"].get(bat_buoc, 0) + 1

        group["sections"].add(section_title)
        if current_khoi:
            group["khoi"].add(current_khoi)

        for g in re.findall(r"\b(?:AV|PV|N\d+)\b", row_text, re.IGNORECASE):
            group["elective_groups"].add(g.upper())

        hp_tq = _row_value_by_label(table, row, "tiên quyết")
        hp_sh = _row_value_by_label(table, row, "song hành")
        for code in COURSE_CODE_RE.findall(hp_tq or ""):
            group["raw_rel"].append(("YEU_CAU_TIEN_QUYET", code.replace(" ", ""), hp_tq))
        for code in COURSE_CODE_RE.findall(hp_sh or ""):
            group["raw_rel"].append(("CO_THE_SONG_HANH", code.replace(" ", ""), hp_sh))

    def pick_most_common(counter: Dict[Any, int]) -> Any:
        if not counter:
            return None
        return max(counter.items(), key=lambda x: x[1])[0]

    for _, group in grouped.items():
        ten_hp = pick_most_common(group["name_counts"])
        item = state.add_node(
            "HocPhan",
            {
                "ma_hoc_phan": group["ma_hp"],
                "ten_hoc_phan": ten_hp,
                "so_tin_chi": pick_most_common(group["so_tc"]),
                "so_tiet_ly_thuyet": pick_most_common(group["so_lt"]),
                "so_tiet_thuc_hanh": pick_most_common(group["so_th"]),
                "tom_tat": pick_most_common(group["tom_tat"]),
                "bat_buoc": pick_most_common(group["bat_buoc"]),
            },
        )

        course_match = group["ma_hp"] or item["ten_hoc_phan"]
        if group["ma_hp"]:
            state.hoc_phan_by_code[_normalize(group["ma_hp"])] = item["ten_hoc_phan"]

        for section_title in sorted(group["sections"]):
            state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "GOM", "HocPhan", course_match, section_title)
        for khoi_name in sorted(group["khoi"]):
            state.add_rel("KhoiKienThuc", khoi_name, "GOM", "HocPhan", course_match, khoi_name)

        for rel_type, target_code, evidence in group["raw_rel"]:
            pending_rel.append((course_match, rel_type, target_code, evidence))

        for group_name in sorted(group["elective_groups"]):
            group_item = state.add_node("NhomHocPhanTuChon", {"ten_nhom": group_name})
            state.add_rel("NhomHocPhanTuChon", group_item["ten_nhom"], "GOM", "HocPhan", course_match, group_name)

    for source_match, rel_type, target_code, evidence in pending_rel:
        target_name = state.hoc_phan_by_code.get(_normalize(target_code), target_code)
        state.add_rel("HocPhan", source_match, rel_type, "HocPhan", target_name, evidence)


def extract_node_yeu_cau_tu_chon(state: GraphState) -> None:
    for sec in state.flat_sections:
        text = _section_text_recursive(sec)
        for line in text.splitlines():
            line = _clean_text(line)
            if not line or "tự chọn" not in _normalize(line):
                continue
            credit = _parse_int(CREDIT_RE.search(line).group(1)) if CREDIT_RE.search(line) else None
            req = state.add_node("YeuCauTuChon", {"noi_dung_yeu_cau": line, "so_tin_chi_yeu_cau": credit})

            for khoi_name in state.khoi_by_norm.values():
                if _normalize(khoi_name) in _normalize(line):
                    state.add_rel("KhoiKienThuc", khoi_name, "CO", "YeuCauTuChon", req["noi_dung_yeu_cau"], line)

            for group in re.findall(r"\b(?:AV|PV|N\d+)\b", line, re.IGNORECASE):
                group = group.upper()
                state.add_node("NhomHocPhanTuChon", {"ten_nhom": group})
                state.add_rel("YeuCauTuChon", req["noi_dung_yeu_cau"], "DOI_VOI", "NhomHocPhanTuChon", group, line)


def _build_base_context(parser_sections: Any, parser_text: str, table_text: str) -> GraphState:
    state = GraphState()
    state.flat_sections = _flatten_sections(parser_sections or [])

    all_chunks = []
    if parser_text:
        all_chunks.append(_clean_text(parser_text))
    if table_text:
        all_chunks.append(_clean_text(table_text))
    all_chunks.extend(sec.get("text_content", "") for sec in state.flat_sections)
    state.full_text = "\n".join(chunk for chunk in all_chunks if chunk).strip()

    _build_info_map(state)
    return state


def extract_curriculum_entities(
    parser_text: str = "",
    table_text: str = "",
    tables: Optional[Any] = None,
    parser_sections: Optional[Any] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    state = _build_base_context(parser_sections, parser_text, table_text)

    extract_node_chuong_trinh_dao_tao(state, source)
    extract_node_van_ban_phap_ly(state)
    extract_node_nganh(state)
    extract_node_khoa(state)
    extract_node_bo_mon(state)
    extract_node_trinh_do(state)
    extract_node_loai_van_bang(state)
    extract_node_hinh_thuc_dao_tao(state)
    extract_node_phuong_thuc_dao_tao(state)
    extract_node_doi_tuong_tuyen_sinh(state)
    extract_node_dieu_kien_tot_nghiep(state)
    extract_node_muc_tieu_dao_tao(state)
    extract_node_chuan_dau_ra(state)
    extract_node_vi_tri_viec_lam(state)
    extract_node_chuan_tham_khao(state)
    extract_node_danh_gia_kiem_dinh(state)
    extract_node_kha_nang_hoc_tap(state)
    extract_node_khoi_kien_thuc(state)
    extract_node_hoc_phan(state)
    extract_node_yeu_cau_tu_chon(state)

    return {
        "source": Path(source).name if source else None,
        "status": "pending_admin_review",
        "created_at": _utc_now_iso(),
        "parser_output": {
            "section_count": len(state.flat_sections),
            "text_chars": len(state.full_text),
            "preview": state.full_text[:1200],
        },
        "table_output": {
            "table_count": sum(len(_as_list(sec.get("tables"))) for sec in state.flat_sections),
            "preview": "",
        },
        "results": state.results,
        "relationships": state.relationships,
    }


def save_json_output(data: Dict[str, Any], path: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=4)
