import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    from app.scripts.term_dictionary import (
        matches_property_key,
        matches_table_label,
        normalize_token,
        resolve_property_key,
        slugify_token,
    )
else:
    from .term_dictionary import (
        matches_property_key,
        matches_table_label,
        normalize_token,
        resolve_property_key,
        slugify_token,
    )


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
        "dieu_kien",
        "yeu_cau_stc_toi_thieu",
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
ELECTIVE_GROUP_RE = re.compile(r"\b(?:AV|PV|N\d+)\b", re.IGNORECASE)
MIN_STC_RE = re.compile(r"(?:>=|≥)\s*(\d{1,3})\s*TC\b", re.IGNORECASE)

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


def _parse_training_duration_years(value: Any) -> Optional[float]:
    text = _clean_text(value)
    if not text:
        return None

    normalized = _normalize(text)

    # Prefer explicit duration phrases, e.g. "thoi gian dao tao: 4.5 nam".
    explicit = re.search(r"thoi gian dao tao\D{0,30}(\d{1,2}(?:[\.,]\d+)?)\s*nam\b", normalized)
    if explicit:
        return float(explicit.group(1).replace(",", "."))

    # Generic "x năm" fallback with sanity bounds to ignore years in dates.
    for match in re.finditer(r"(\d{1,2}(?:[\.,]\d+)?)\s*(?:nam|năm)\b", text, re.IGNORECASE):
        years = float(match.group(1).replace(",", "."))
        if 2.0 <= years <= 8.0:
            return years

    # Some files encode duration by semesters instead of years.
    semester_match = re.search(r"(\d{1,2})\s*(?:hoc\s*ky|học\s*kỳ)", normalized)
    if semester_match:
        semesters = int(semester_match.group(1))
        if 4 <= semesters <= 16:
            return semesters / 2.0

    return None


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


def _extract_min_stc_requirement(text: str) -> Optional[str]:
    match = MIN_STC_RE.search(_clean_text(text))
    if not match:
        return None
    return f"≥ {match.group(1)} TC"


def _extract_course_codes(text: str) -> List[str]:
    return [code.replace(" ", "") for code in COURSE_CODE_RE.findall(_clean_text(text))]


def _text_or_none(value: Any) -> Optional[str]:
    text = _clean_text(value)
    return text if text else None


def _sentences(*parts: Optional[str]) -> str:
    cleaned: List[str] = []
    for part in parts:
        if not part:
            continue
        sentence = _clean_text(part)
        if not sentence:
            continue
        sentence = sentence.rstrip(";,: ")
        if sentence[-1] not in ".!?":
            sentence = f"{sentence}."
        cleaned.append(sentence)
    return " ".join(cleaned).strip()


def _compose_node_text(node_type: str, payload: Dict[str, Any]) -> str:
    if node_type == "ChuongTrinhDaoTao":
        ma = _text_or_none(payload.get("ma_chuong_trinh")) or "UNKNOWN"
        khoa = _text_or_none(payload.get("khoa"))
        he = _text_or_none(payload.get("he"))
        ngon_ngu = _text_or_none(payload.get("ngon_ngu"))
        tong_tc = _text_or_none(payload.get("tong_tin_chi"))
        thoi_gian = _text_or_none(payload.get("thoi_gian_dao_tao"))
        thang_diem = _text_or_none(payload.get("thang_diem"))
        return _sentences(
            f"Chương trình đào tạo có mã {ma}",
            f"Khóa tuyển sinh là {khoa}" if khoa else None,
            f"Hệ đào tạo là {he}" if he else None,
            f"Ngôn ngữ giảng dạy là {ngon_ngu}" if ngon_ngu else None,
            f"Tổng số tín chỉ toàn chương trình là {tong_tc}" if tong_tc else None,
            f"Thời gian đào tạo dự kiến là {thoi_gian} năm" if thoi_gian else None,
            f"Thang điểm đánh giá sử dụng là {thang_diem}" if thang_diem else None,
        )

    if node_type == "Nganh":
        ma = _text_or_none(payload.get("ma_nganh"))
        ten_vi = _text_or_none(payload.get("ten_nganh_vi"))
        ten_en = _text_or_none(payload.get("ten_nganh_en"))
        return _sentences(
            f"Ngành đào tạo là {ten_vi}" if ten_vi else "Đây là một ngành đào tạo",
            f"Mã ngành là {ma}" if ma else None,
            f"Tên ngành bằng tiếng Anh là {ten_en}" if ten_en else None,
        )

    if node_type == "Khoa":
        ma = _text_or_none(payload.get("ma_khoa"))
        ten = _text_or_none(payload.get("ten_khoa"))
        return _sentences(
            f"Khoa quản lý là {ten}" if ten else "Khoa quản lý chưa được xác định",
            f"Mã khoa là {ma}" if ma else None,
        )

    if node_type == "BoMon":
        ma = _text_or_none(payload.get("ma_bo_mon"))
        ten = _text_or_none(payload.get("ten_bo_mon"))
        return _sentences(
            f"Bộ môn phụ trách là {ten}" if ten else "Bộ môn phụ trách chưa được xác định",
            f"Mã bộ môn là {ma}" if ma else None,
        )

    if node_type == "TrinhDo":
        ten = _text_or_none(payload.get("ten_trinh_do"))
        ma = _text_or_none(payload.get("ma_trinh_do"))
        return _sentences(
            f"Trình độ đào tạo là {ten}" if ten else "Trình độ đào tạo chưa xác định",
            f"Mã trình độ là {ma}" if ma else None,
        )

    if node_type == "LoaiVanBang":
        loai = _text_or_none(payload.get("loai_van_bang"))
        ma = _text_or_none(payload.get("ma_loai"))
        return _sentences(
            f"Loại văn bằng được cấp là {loai}" if loai else "Loại văn bằng chưa xác định",
            f"Mã loại văn bằng là {ma}" if ma else None,
        )

    if node_type == "HinhThucDaoTao":
        ten = _text_or_none(payload.get("ten_hinh_thuc"))
        ma = _text_or_none(payload.get("ma_hinh_thuc"))
        return _sentences(
            f"Hình thức đào tạo là {ten}" if ten else "Hình thức đào tạo chưa xác định",
            f"Mã hình thức đào tạo là {ma}" if ma else None,
        )

    if node_type == "PhuongThucDaoTao":
        ten = _text_or_none(payload.get("ten_phuong_thuc"))
        ma = _text_or_none(payload.get("ma_phuong_thuc"))
        return _sentences(
            f"Phương thức đào tạo là {ten}" if ten else "Phương thức đào tạo chưa xác định",
            f"Mã phương thức đào tạo là {ma}" if ma else None,
        )

    if node_type == "VanBanPhapLy":
        ten = _text_or_none(payload.get("ten"))
        so = _text_or_none(payload.get("so"))
        ngay = _text_or_none(payload.get("ngay_ban_hanh"))
        co_quan = _text_or_none(payload.get("co_quan_ban_hanh"))
        return _sentences(
            f"Văn bản pháp lý tham chiếu là {ten}" if ten else "Văn bản pháp lý tham chiếu chưa xác định",
            f"Số hiệu văn bản là {so}" if so else None,
            f"Ngày ban hành là {ngay}" if ngay else None,
            f"Cơ quan ban hành là {co_quan}" if co_quan else None,
        )

    if node_type == "DoiTuongTuyenSinh":
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(f"Đối tượng tuyển sinh là {noi_dung}" if noi_dung else "Đối tượng tuyển sinh chưa xác định")

    if node_type == "DieuKienTotNghiep":
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(f"Điều kiện tốt nghiệp gồm: {noi_dung}" if noi_dung else "Điều kiện tốt nghiệp chưa xác định")

    if node_type == "MucTieuDaoTao":
        loai = _text_or_none(payload.get("loai"))
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(
            f"Đây là mục tiêu đào tạo loại {loai}" if loai else "Đây là mục tiêu đào tạo",
            noi_dung,
        )

    if node_type == "ChuanDauRa":
        ma = _text_or_none(payload.get("ma_chuan"))
        nhom = _text_or_none(payload.get("nhom"))
        loai = _text_or_none(payload.get("loai"))
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(
            f"Chuẩn đầu ra {ma} thuộc nhóm {nhom} và loại {loai}" if (ma and nhom and loai) else None,
            f"Chuẩn đầu ra thuộc nhóm {nhom} và loại {loai}" if (not ma and nhom and loai) else None,
            f"Chuẩn đầu ra có mã {ma}" if (ma and not nhom and not loai) else None,
            "Đây là một chuẩn đầu ra của chương trình" if (not ma and not nhom and not loai) else None,
            noi_dung,
        )

    if node_type == "ViTriViecLam":
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(f"Vị trí việc làm sau tốt nghiệp bao gồm: {noi_dung}" if noi_dung else "Vị trí việc làm chưa xác định")

    if node_type == "ChuanThamKhao":
        noi_dung = _text_or_none(payload.get("noi_dung"))
        link = _text_or_none(payload.get("link"))
        return _sentences(
            f"Chuẩn tham khảo là {noi_dung}" if noi_dung else "Chuẩn tham khảo",
            f"Liên kết tham khảo: {link}" if link else None,
        )

    if node_type == "DanhGiaKiemDinh":
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(f"Thông tin đánh giá kiểm định: {noi_dung}" if noi_dung else "Thông tin đánh giá kiểm định chưa xác định")

    if node_type == "KhaNangHocTap":
        noi_dung = _text_or_none(payload.get("noi_dung"))
        return _sentences(f"Khả năng học tập nâng cao sau tốt nghiệp: {noi_dung}" if noi_dung else "Khả năng học tập nâng cao chưa xác định")

    if node_type == "KhoiKienThuc":
        ma = _text_or_none(payload.get("ma_khoi"))
        ten = _text_or_none(payload.get("ten_khoi"))
        tong = _text_or_none(payload.get("tong_tin_chi"))
        bb = _text_or_none(payload.get("tin_chi_bat_buoc"))
        tc = _text_or_none(payload.get("tin_chi_tu_chon"))
        return _sentences(
            f"Khối kiến thức là {ten}" if ten else "Khối kiến thức chưa xác định",
            f"Mã khối kiến thức là {ma}" if ma else None,
            f"Tổng số tín chỉ của khối là {tong}" if tong else None,
            f"Số tín chỉ bắt buộc là {bb}" if bb else None,
            f"Số tín chỉ tự chọn là {tc}" if tc else None,
        )

    if node_type == "HocPhan":
        ma = _text_or_none(payload.get("ma_hoc_phan"))
        ten = _text_or_none(payload.get("ten_hoc_phan"))
        so_tc = _text_or_none(payload.get("so_tin_chi"))
        so_lt = _text_or_none(payload.get("so_tiet_ly_thuyet"))
        so_th = _text_or_none(payload.get("so_tiet_thuc_hanh"))
        tom_tat = _text_or_none(payload.get("tom_tat"))
        bat_buoc = payload.get("bat_buoc")
        dieu_kien = payload.get("dieu_kien")
        yeu_cau_stc = _text_or_none(payload.get("yeu_cau_stc_toi_thieu"))
        bat_buoc_text = None
        if isinstance(bat_buoc, bool):
            bat_buoc_text = "Đây là học phần bắt buộc" if bat_buoc else "Đây là học phần tự chọn"

        return _sentences(
            f"Học phần {ma} có tên là {ten}" if (ma and ten) else (f"Học phần có tên là {ten}" if ten else "Học phần"),
            f"Số tín chỉ của học phần là {so_tc}" if so_tc else None,
            f"Số tiết lý thuyết là {so_lt}" if so_lt else None,
            f"Số tiết thực hành là {so_th}" if so_th else None,
            "Học phần này có điều kiện học trước" if dieu_kien else None,
            f"Yêu cầu tín chỉ tối thiểu: {yeu_cau_stc}" if yeu_cau_stc else None,
            bat_buoc_text,
            f"Mô tả tóm tắt học phần: {tom_tat}" if tom_tat else None,
        )

    if node_type == "NhomHocPhanTuChon":
        ten = _text_or_none(payload.get("ten_nhom"))
        return _sentences(f"Nhóm học phần tự chọn là {ten}" if ten else "Nhóm học phần tự chọn chưa xác định")

    if node_type == "YeuCauTuChon":
        noi_dung = _text_or_none(payload.get("noi_dung_yeu_cau"))
        so_tc = _text_or_none(payload.get("so_tin_chi_yeu_cau"))
        return _sentences(
            f"Yêu cầu tự chọn: {noi_dung}" if noi_dung else "Yêu cầu tự chọn",
            f"Số tín chỉ tự chọn cần đạt là {so_tc}" if so_tc else None,
        )

    return node_type


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
        custom_text = _clean_text(values.get("text"))
        payload["text"] = custom_text if custom_text else _compose_node_text(node_type, payload)
        signature_payload = dict(payload)
        dedupe_key = _clean_text(values.get("_dedupe_key"))
        if dedupe_key:
            signature_payload["_dedupe_key"] = dedupe_key
        signature = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True, default=str)
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


def _is_framework_section_title(title: str) -> bool:
    norm_title = _normalize(title)
    return "khung chuong trinh dao tao" in norm_title


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


def _table_raw_rows(table: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_rows = table.get("raw_rows") or []
    if raw_rows:
        converted: List[Dict[str, Any]] = []
        for row in raw_rows:
            if isinstance(row, dict):
                converted.append(row)
            elif isinstance(row, (list, tuple)):
                converted.append({f"col_{index + 1}": value for index, value in enumerate(row)})

        # Align raw rows with records (records usually exclude the header row).
        records = _table_rows(table)
        if records and len(converted) == len(records) + 1:
            converted = converted[1:]
        return converted
    return _table_rows(table)


def _table_columns(table: Dict[str, Any]) -> List[str]:
    cols = []
    for col in table.get("columns") or []:
        cols.append(_clean_text(col))
    return cols


def _row_value_by_label(table: Dict[str, Any], row: Dict[str, Any], contains: str) -> str:
    cols = _table_columns(table)
    for idx, label in enumerate(cols, start=1):
        if matches_table_label(label, contains):
            return _clean_text(row.get(f"col_{idx}", ""))

    # Backward-compatible fallback for ad-hoc lookups.
    target = _normalize(contains)
    for idx, label in enumerate(cols, start=1):
        if target and target in _normalize(label):
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
                norm_key = _normalize(key)
                state.info_map[norm_key] = val
                canonical_key = resolve_property_key(key)
                if canonical_key:
                    state.info_map[canonical_key] = val


def _info_get(state: GraphState, *canonical_keys: str) -> Optional[str]:
    for canonical in canonical_keys:
        value = state.info_map.get(canonical)
        if value:
            return value

    for key, value in state.info_map.items():
        if not value:
            continue
        for canonical in canonical_keys:
            if matches_property_key(key, canonical):
                return value
    return None


def _info_get_training_duration(state: GraphState) -> Optional[str]:
    direct = state.info_map.get("thoi_gian_dao_tao")
    if direct and _parse_training_duration_years(direct) is not None:
        return direct

    # OCR can corrupt labels; prefer any "thoi gian"-like key whose value parses as duration.
    candidates: List[str] = []
    for key, value in state.info_map.items():
        if not value:
            continue
        key_norm = _normalize(key)
        value_clean = _clean_text(value)
        if not value_clean:
            continue

        looks_like_duration_key = (
            "thoi gian dao tao" in key_norm
            or ("thoi gian" in key_norm and ("dao tao" in key_norm or "khoa hoc" in key_norm or "chuong trinh" in key_norm))
            or matches_property_key(key, "thoi_gian_dao_tao")
        )
        if looks_like_duration_key and _parse_training_duration_years(value_clean) is not None:
            candidates.append(value_clean)

    if candidates:
        # Prefer the first candidate from info table order.
        return candidates[0]

    # Last-resort fallback in info map only: key mentions "thoi gian" and value parses.
    for key, value in state.info_map.items():
        key_norm = _normalize(key)
        if "thoi gian" not in key_norm:
            continue
        value_clean = _clean_text(value)
        if _parse_training_duration_years(value_clean) is not None:
            return value_clean
    return None


def _extract_major_name_from_text(full_text: str) -> Optional[str]:
    text = _clean_text(full_text)
    if not text:
        return None
    match = re.search(
        r"\bNgành\s*:\s*([^\n:]+?)(?:\s*\(|\s+Mã\s*ngành\s*:|\n|$)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    return _clean_text(match.group(1))


def _extract_major_code_from_text(full_text: str) -> Optional[str]:
    text = _clean_text(full_text)
    if not text:
        return None
    match = re.search(r"\bMã\s*ngành\s*:\s*(\d{6,10})\b", text, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _extract_total_credits_from_text(full_text: str) -> Optional[int]:
    text = _clean_text(full_text)
    if not text:
        return None
    match = re.search(r"\bSố\s*lượng\s*tín\s*chỉ\s*:\s*(\d{2,3})\b", text, re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _extract_training_duration_text(full_text: str) -> Optional[str]:
    text = _clean_text(full_text)
    if not text:
        return None
    match = re.search(
        r"\bThời\s*gian\s*đào\s*tạo\s*:\s*([^\n]+?)(?=\s+Loại\s+văn\s+bằng\s*:|\n|$)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    return _clean_text(match.group(1))


def extract_node_chuong_trinh_dao_tao(state: GraphState, source: Optional[str]) -> None:
    ma_nganh = None
    ten_vi = _info_get(state, "ten_nganh_vi")
    if not ten_vi:
        ten_vi = _extract_major_name_from_text(state.full_text)
    tong_tc = _parse_int(_info_get(state, "tong_tin_chi") or "")
    if tong_tc is None:
        tong_tc = _extract_total_credits_from_text(state.full_text)
    raw_thoi_gian = _info_get_training_duration(state) or ""
    if not raw_thoi_gian:
        raw_thoi_gian = _extract_training_duration_text(state.full_text) or ""
    thoi_gian = _parse_training_duration_years(raw_thoi_gian)
    thang_diem = _parse_float(_info_get(state, "thang_diem") or "")

    value_ma_nganh = _info_get(state, "ma_nganh")
    if not value_ma_nganh:
        value_ma_nganh = _extract_major_code_from_text(state.full_text)
    if value_ma_nganh:
        m = re.search(r"\d{4,10}", _clean_text(value_ma_nganh))
        ma_nganh = m.group(0) if m else None

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
    ten_vi = _info_get(state, "ten_nganh_vi")
    if not ten_vi:
        ten_vi = _extract_major_name_from_text(state.full_text)
    
    ten_en = _info_get(state, "ten_nganh_en")
    
    ma_nganh = _parse_int(_info_get(state, "ma_nganh") or "")
    if ma_nganh is None:
        ma_nganh = _parse_int(_extract_major_code_from_text(state.full_text) or "")

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
    management_unit = _info_get(state, "don_vi_quan_ly")
    khoa_from_info = None
    for key, value in state.info_map.items():
        norm_key = _normalize(key)
        if norm_key.startswith("khoa") and not matches_property_key(key, "khoa"):
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
    bo_mon_name = _clean_text(_info_get(state, "bo_mon") or "")

    if not bo_mon_name:
        cover_section = _find_section(state, "Trang bìa")
        cover_text = _section_text_recursive(cover_section) if cover_section else state.full_text
        for line in cover_text.splitlines():
            line_clean = _clean_text(line)
            if _normalize(line_clean).startswith("bo mon"):
                bo_mon_name = line_clean
                break

    if not bo_mon_name:
        return

    if _normalize(bo_mon_name) in {"bo mon", "bo mon phu trach", "bo mon phu trach la"}:
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
    value = _info_get(state, "trinh_do_dao_tao")
    if not value:
        return

    item = state.add_node("TrinhDo", {"ma_trinh_do": slugify_token(value), "ten_trinh_do": value})
    state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "DAO_TAO", "TrinhDo", item["ten_trinh_do"], value)


def extract_node_loai_van_bang(state: GraphState) -> None:
    value = _info_get(state, "ten_goi_van_bang")
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
    value = _info_get(state, "hinh_thuc_dao_tao")
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
    value = _info_get(state, "doi_tuong_tuyen_sinh")
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
    value = _info_get(state, "dieu_kien_tot_nghiep")
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
    value = _info_get(state, "vi_tri_viec_lam")
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
    value = _info_get(state, "danh_gia_kiem_dinh")

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
        program_code = _clean_text(state.program_id or "") or "UNKNOWN"
        item = state.add_node(
            "KhoiKienThuc",
            {
                "ma_khoi": f"{program_code}_{slugify_token(ten_khoi)}",
                "ten_khoi": ten_khoi,
                "tong_tin_chi": int(m.group(2)),
                "tin_chi_bat_buoc": int(m.group(3)),
                "tin_chi_tu_chon": int(m.group(4)),
            },
        )
        key = _normalize(item["ten_khoi"])
        state.khoi_by_norm[key] = item["ten_khoi"]
        state.add_rel("ChuongTrinhDaoTao", state.ctdt_match, "GOM", "KhoiKienThuc", item["ten_khoi"], m.group(0))


def _extract_course_rows(state: GraphState) -> List[Tuple[Dict[str, Any], Dict[str, Any], str, int]]:
    rows: List[Tuple[Dict[str, Any], Dict[str, Any], str, int]] = []
    for sec in state.flat_sections:
        title_norm = _normalize(sec.get("title", ""))
        if "khung chuong trinh dao tao" not in title_norm and "ke hoach day hoc" not in title_norm and "mo ta tom tat" not in title_norm:
            continue
        for table in sec.get("tables", []):
            for row_index, row in enumerate(_table_rows(table)):
                rows.append((table, row, sec.get("title", ""), row_index))
    return rows


def extract_node_hoc_phan(state: GraphState) -> None:
    pending_rel: List[Tuple[str, str, str, str]] = []
    current_khoi: Optional[str] = None
    current_scope: Optional[Tuple[str, int]] = None

    # Group raw rows by course identity first to avoid duplicate HocPhan nodes.
    grouped: Dict[str, Dict[str, Any]] = {}

    for table, row, section_title, row_index in _extract_course_rows(state):
        in_framework_section = _is_framework_section_title(section_title)
        scope = (_clean_text(section_title), id(table))
        if scope != current_scope:
            current_scope = scope
            current_khoi = None

        row_text = " | ".join(_clean_text(v) for v in row.values() if _clean_text(v))
        if not row_text:
            continue

        if "khoi kien thuc" in _normalize(row_text):
            for khoi_key, khoi_name in state.khoi_by_norm.items():
                if khoi_key in _normalize(row_text):
                    current_khoi = khoi_name
                    break
            continue

        ma_hp = _row_value_by_label(table, row, "ma_hoc_phan") or row.get("col_2", "")
        ma_hp = _clean_text(ma_hp).replace(" ", "")
        if ma_hp and not COURSE_CODE_RE.search(ma_hp):
            ma_hp = ""

        ten_hp = _row_value_by_label(table, row, "ten_hoc_phan") or row.get("col_3", "")
        ten_hp = _clean_text(ten_hp)
        if not ten_hp:
            continue

        so_tc = _parse_int(_row_value_by_label(table, row, "so_tin_chi") or row.get("col_4", ""))
        so_lt = _parse_int(_row_value_by_label(table, row, "ly_thuyet") or row.get("col_7", ""))
        so_th = _parse_int(_row_value_by_label(table, row, "thuc_hanh") or row.get("col_8", ""))
        tom_tat = _row_value_by_label(table, row, "mo_ta_tom_tat")
        hp_tq = _row_value_by_label(table, row, "tien_quyet")
        min_stc = _extract_min_stc_requirement(hp_tq or "")

        bat_buoc = None
        if _parse_int(_row_value_by_label(table, row, "bat_buoc") or row.get("col_5", "")):
            bat_buoc = True
        elif _parse_int(_row_value_by_label(table, row, "tu_chon") or row.get("col_6", "")):
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
                "dieu_kien": False,
                "yeu_cau_stc_toi_thieu": {},
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
        if "*" in ten_hp:
            group["dieu_kien"] = True
        if min_stc:
            group["yeu_cau_stc_toi_thieu"][min_stc] = group["yeu_cau_stc_toi_thieu"].get(min_stc, 0) + 1
        if tom_tat:
            group["tom_tat"][tom_tat] = group["tom_tat"].get(tom_tat, 0) + 1
        if bat_buoc is not None:
            group["bat_buoc"][bat_buoc] = group["bat_buoc"].get(bat_buoc, 0) + 1

        group["sections"].add(section_title)
        if current_khoi:
            group["khoi"].add(current_khoi)

        # Elective groups are only extracted from framework curriculum tables.
        if in_framework_section:
            group_source = _row_value_by_label(table, row, "bat_buoc") or row.get("col_5", "")
            for g in ELECTIVE_GROUP_RE.findall(_clean_text(group_source)):
                group_display = _clean_text(group_source or g)
                if group_display:
                    group["elective_groups"].add((group_display, _normalize(group_display)))

        hp_sh = _row_value_by_label(table, row, "song_hanh")
        for code in COURSE_CODE_RE.findall(hp_tq or ""):
            group["raw_rel"].append(("YEU_CAU_TIEN_QUYET", code.replace(" ", ""), hp_tq))
        for code in COURSE_CODE_RE.findall(hp_sh or ""):
            group["raw_rel"].append(("CO_THE_SONG_HANH", code.replace(" ", ""), hp_sh))

    def pick_most_common(counter: Dict[Any, int]) -> Any:
        if not counter:
            return None
        return max(counter.items(), key=lambda x: x[1])[0]

    def pick_course_name(counter: Dict[str, int], prefer_starred: bool) -> Optional[str]:
        if not counter:
            return None
        if prefer_starred:
            starred = [name for name in counter if "*" in name]
            if starred:
                return max(starred, key=lambda name: counter[name])
        return pick_most_common(counter)

    for _, group in grouped.items():
        ten_hp = pick_course_name(group["name_counts"], bool(group["dieu_kien"]))
        item = state.add_node(
            "HocPhan",
            {
                "ma_hoc_phan": group["ma_hp"],
                "ten_hoc_phan": ten_hp,
                "so_tin_chi": pick_most_common(group["so_tc"]),
                "so_tiet_ly_thuyet": pick_most_common(group["so_lt"]),
                "so_tiet_thuc_hanh": pick_most_common(group["so_th"]),
                "tom_tat": pick_most_common(group["tom_tat"]),
                "dieu_kien": group["dieu_kien"],
                "yeu_cau_stc_toi_thieu": pick_most_common(group["yeu_cau_stc_toi_thieu"]),
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

        for group_name, group_key in sorted(group["elective_groups"]):
            group_item = state.add_node(
                "NhomHocPhanTuChon",
                {
                    "ten_nhom": group_name,
                    "_dedupe_key": group_key,
                },
            )
            state.add_rel("NhomHocPhanTuChon", group_item["ten_nhom"], "GOM", "HocPhan", course_match, group_name)

    for source_match, rel_type, target_code, evidence in pending_rel:
        target_name = state.hoc_phan_by_code.get(_normalize(target_code), target_code)
        state.add_rel("HocPhan", source_match, rel_type, "HocPhan", target_name, evidence)


def extract_node_yeu_cau_tu_chon(state: GraphState) -> None:
    seen: set = set()

    def parse_groups(*chunks: str) -> List[str]:
        merged = " ".join(_clean_text(chunk) for chunk in chunks if _clean_text(chunk))
        groups = ELECTIVE_GROUP_RE.findall(merged)
        deduped: List[str] = []
        group_seen = set()
        for group in groups:
            token = group.upper()
            if token in group_seen:
                continue
            group_seen.add(token)
            deduped.append(token)
        return deduped

    def parse_credit(*chunks: str) -> Optional[int]:
        for chunk in chunks:
            text = _clean_text(chunk)
            if not text:
                continue
            m_credit = CREDIT_RE.search(text)
            if m_credit:
                return _parse_int(m_credit.group(1))
            if re.fullmatch(r"\d{1,3}", text):
                return int(text)
        return None

    def add_requirement(
        value_text: str,
        evidence: str,
        context_text: str,
        groups: Sequence[str],
        credit: Optional[int],
        from_course_row: bool,
        khoi_matches: Sequence[str],
        course_codes: Sequence[str],
    ) -> None:
        value_text = _clean_text(value_text)
        evidence = _clean_text(evidence)
        context_text = _clean_text(context_text)
        if not value_text:
            return

        value_norm = _normalize(value_text)

        normalized_groups = tuple(sorted(set(groups)))
        signature = json.dumps(
            {
                "value": value_norm,
                "groups": normalized_groups,
                "credit": credit,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if signature in seen:
            return
        seen.add(signature)

        req = state.add_node(
            "YeuCauTuChon",
            {
                "noi_dung_yeu_cau": value_text,
                "so_tin_chi_yeu_cau": credit,
            },
        )

        linked_courses: List[str] = []
        if from_course_row:
            for code in course_codes:
                course_match = state.hoc_phan_by_code.get(_normalize(code), code)
                if course_match and course_match not in linked_courses:
                    linked_courses.append(course_match)

        for group in normalized_groups:
            group_item = state.add_node(
                "NhomHocPhanTuChon",
                {
                    "ten_nhom": group,
                    "_dedupe_key": _normalize(group),
                },
            )
            state.add_rel(
                "YeuCauTuChon",
                req["noi_dung_yeu_cau"],
                "DOI_VOI",
                "NhomHocPhanTuChon",
                group_item["ten_nhom"],
                evidence,
            )

        for course_match in linked_courses:
            state.add_rel(
                "YeuCauTuChon",
                req["noi_dung_yeu_cau"],
                "GOM",
                "HocPhan",
                course_match,
                evidence or value_text,
            )

    for sec in state.flat_sections:
        section_title = _clean_text(sec.get("title", ""))
        if not _is_framework_section_title(section_title):
            continue

        section_text = _section_text_recursive(sec)
        section_context = _compact(f"{section_title} {section_text}")

        for table in sec.get("tables", []):
            current_khoi: Optional[str] = None
            cols = _table_columns(table)
            tu_chon_idx = next((idx for idx, label in enumerate(cols, start=1) if matches_table_label(label, "tu_chon")), None)
            bat_buoc_idx = next((idx for idx, label in enumerate(cols, start=1) if matches_table_label(label, "bat_buoc")), None)
            ma_hp_idx = next(
                (
                    idx
                    for idx, label in enumerate(cols, start=1)
                    if matches_table_label(label, "ma_hoc_phan")
                ),
                None,
            )

            if not tu_chon_idx:
                continue

            # Use normalized records so merged cells are propagated consistently across rows.
            for row in _table_rows(table):
                elective_value = _clean_text(row.get(f"col_{tu_chon_idx}", ""))
                if not elective_value:
                    continue

                row_text = " | ".join(_clean_text(v) for v in row.values() if _clean_text(v))
                row_norm = _normalize(row_text)

                if "khoi kien thuc" in row_norm:
                    for khoi_key, khoi_name in state.khoi_by_norm.items():
                        if khoi_key in row_norm:
                            current_khoi = khoi_name
                            break

                code_value = _clean_text(row.get(f"col_{ma_hp_idx}", "")) if ma_hp_idx else ""
                from_course_row = bool(COURSE_CODE_RE.search(code_value or row_text))
                course_codes = _extract_course_codes(code_value) if code_value else []

                # Business rule: take groups from "Bắt buộc" column when it has alphabetic tokens.
                group_source = _clean_text(row.get(f"col_{bat_buoc_idx}", "")) if bat_buoc_idx else ""
                groups = parse_groups(group_source)
                if not groups:
                    # Fallback for files that encode group inside elective text.
                    groups = parse_groups(elective_value)
                groups = [_clean_text(g) for g in groups if _clean_text(g)]
                credit = parse_credit(elective_value)
                context_text = _compact(f"{section_context} {row_text}")
                khoi_matches: List[str] = []
                if current_khoi:
                    khoi_matches.append(current_khoi)

                add_requirement(
                    value_text=elective_value,
                    evidence=row_text or elective_value,
                    context_text=context_text,
                    groups=groups,
                    credit=credit,
                    from_course_row=from_course_row,
                    khoi_matches=[],
                    course_codes=course_codes,
                )


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
