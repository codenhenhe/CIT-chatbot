import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple

# Canonicalized key terms used by parser/extractor/table modules.
TERM_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\badmission\s+requirements?\b", re.IGNORECASE), "doi tuong tuyen sinh"),
    (re.compile(r"\bprogram\s+objectives?\b", re.IGNORECASE), "muc tieu dao tao"),
    (re.compile(r"\bcareer\s+opportunities?\b", re.IGNORECASE), "vi tri viec lam"),
    (re.compile(r"\blearning\s+outcomes?\b", re.IGNORECASE), "chuan dau ra"),
    (re.compile(r"\blifelong\s+learning\b", re.IGNORECASE), "kha nang hoc tap"),
    (re.compile(r"\b(accreditation|quality\s+assurance)\b", re.IGNORECASE), "danh gia kiem dinh"),
    (re.compile(r"\b(admission|entry\s+requirements?)\b", re.IGNORECASE), "doi tuong tuyen sinh"),
    (re.compile(r"\bgraduation\s+requirements?\b", re.IGNORECASE), "dieu kien tot nghiep"),
    (re.compile(r"\b(reference\s+standards?|references?)\b", re.IGNORECASE), "chuan tham khao"),
    (re.compile(r"\bcourse\s*id\b", re.IGNORECASE), "ma hp"),
    (re.compile(r"\bcourse\s*title\b", re.IGNORECASE), "ten hp"),
    (re.compile(r"\bcredits?\b", re.IGNORECASE), "tin chi"),
    (re.compile(r"\bprerequisites?\b", re.IGNORECASE), "tien quyet"),
    (re.compile(r"\bco-?requisites?\b", re.IGNORECASE), "song hanh"),
]

TABLE_RELEVANT_HINTS = {
    "stt",
    "ma hp",
    "ma hoc phan",
    "ten hoc phan",
    "tin chi",
    "tien quyet",
    "song hanh",
    "khoi kien thuc",
    "tu chon",
    "gom hoc phan",
}

# Canonical node labels mapped to common Vietnamese/English aliases.
NODE_ALIASES = {
    "ChuongTrinhDaoTao": ["chuong trinh dao tao", "ctdt", "program", "curriculum"],
    "Nganh": ["nganh", "major", "discipline"],
    "Khoa": ["khoa", "faculty", "school"],
    "BoMon": ["bo mon", "department"],
    "TrinhDo": ["trinh do", "level", "academic level"],
    "LoaiVanBang": ["loai van bang", "danh hieu", "degree type"],
    "HinhThucDaoTao": ["hinh thuc dao tao", "mode of study"],
    "PhuongThucDaoTao": ["phuong thuc dao tao", "delivery mode"],
    "DoiTuongTuyenSinh": ["doi tuong tuyen sinh", "admission requirements", "entry requirements"],
    "DieuKienTotNghiep": ["dieu kien tot nghiep", "graduation requirements"],
    "MucTieuDaoTao": ["muc tieu dao tao", "program objectives"],
    "ViTriViecLam": ["vi tri viec lam", "career opportunities", "job positions"],
    "KhaNangHocTap": ["kha nang hoc tap", "lifelong learning"],
    "DanhGiaKiemDinh": ["danh gia kiem dinh", "quality assurance", "accreditation"],
    "ChuanThamKhao": ["chuan tham khao", "references", "reference standards"],
    "ChuanDauRa": ["chuan dau ra", "plo", "learning outcomes"],
    "KhoiKienThuc": ["khoi kien thuc", "knowledge block"],
    "YeuCauTuChon": ["yeu cau tu chon", "elective requirement"],
    "NhomHocPhanTuChon": ["nhom hoc phan tu chon", "elective group"],
    "HocPhan": ["hoc phan", "course", "module"],
    "VanBanPhapLy": ["van ban phap ly", "quyet dinh", "thong tu", "legal document"],
}

# Canonical property names mapped to common aliases that appear in PDFs.
PROPERTY_ALIASES = {
    "ma_nganh": ["ma nganh", "ma so nganh", "major code", "discipline code"],
    "ten_nganh_vi": [
        "nganh",
        "ten nganh",
        "ten chuong trinh tieng viet",
        "ten chuong trinh",
        "vietnamese major",
        "program name vietnamese",
    ],
    "ten_nganh_en": ["major", "ten chuong trinh tieng anh", "english major", "program name english"],
    "ma_chuong_trinh": ["ma chuong trinh", "program code", "curriculum code", "ma ctdt"],
    "khoa": ["khoa", "khoa tuyen", "cohort", "intake"],
    "he": ["he", "he dao tao", "program type", "chuong trinh"],
    "ngon_ngu": ["ngon ngu", "ngon ngu giang day", "language", "teaching language"],
    "tong_tin_chi": ["tong tin chi", "so tin chi", "tong so tin chi", "credits", "total credits"],
    "thoi_gian_dao_tao": ["thoi gian dao tao", "thoi gian", "duration", "study duration"],
    "thang_diem": ["thang diem", "grading scale", "he diem"],
    "don_vi_quan_ly": ["don vi quan ly", "don vi phu trach", "managing unit", "faculty in charge"],
    "bo_mon": ["bo mon", "bo mon phu trach", "department", "unit in charge"],
    "trinh_do_dao_tao": ["trinh do dao tao", "trinh do", "academic level"],
    "ten_goi_van_bang": ["ten goi van bang", "loai van bang", "degree title", "degree type"],
    "hinh_thuc_dao_tao": ["hinh thuc dao tao", "mode of study", "study mode"],
    "doi_tuong_tuyen_sinh": ["doi tuong tuyen sinh", "admission requirements", "entry requirements"],
    "dieu_kien_tot_nghiep": ["dieu kien tot nghiep", "graduation requirements"],
    "vi_tri_viec_lam": ["vi tri viec lam", "career opportunities", "job positions"],
    "danh_gia_kiem_dinh": ["danh gia kiem dinh", "accreditation", "quality assurance"],
    "ma_hp": ["ma hp", "ma hoc phan", "ma so hp", "course id", "course code"],
    "ten_hoc_phan": ["ten hoc phan", "ten hp", "course title", "course name"],
    "ten_hp": ["ten hp", "ten hoc phan", "course title"],
}

TABLE_LABEL_ALIASES = {
    "ma_hoc_phan": ["ma hoc phan", "ma hp", "ma so hp", "ma so hoc phan", "course id", "course code"],
    "ten_hoc_phan": ["ten hoc phan", "ten hp", "ten mon hoc", "course title", "course name"],
    "so_tin_chi": ["so tin chi", "tin chi", "tc", "credits"],
    "bat_buoc": ["bat buoc", "bb", "required", "compulsory"],
    "tu_chon": ["tu chon", "elective", "optional"],
    "mo_ta_tom_tat": ["mo ta tom tat", "tom tat", "noi dung", "description", "summary"],
    "ly_thuyet": ["ly thuyet", "so tiet ly thuyet", "lt", "lecture"],
    "thuc_hanh": ["thuc hanh", "so tiet thuc hanh", "th", "practice", "lab"],
    "tien_quyet": ["tien quyet", "hoc phan tien quyet", "prerequisite", "prerequisites"],
    "song_hanh": ["song hanh", "hoc phan song hanh", "corequisite", "co requisite"],
}


def _normalized_aliases(aliases: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for alias in aliases:
        normalized = normalize_token(alias)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


_PROPERTY_ALIAS_INDEX: Dict[str, List[str]] = {}

_TABLE_ALIAS_INDEX: Dict[str, List[str]] = {}


def _token_set(text: str) -> Set[str]:
    normalized = normalize_token(text)
    if not normalized:
        return set()
    return set(normalized.split())


def _alias_score(text_norm: str, alias_norm: str) -> int:
    if not text_norm or not alias_norm:
        return 0
    if text_norm == alias_norm:
        return 100

    text_tokens = set(text_norm.split())
    alias_tokens = set(alias_norm.split())

    if len(alias_norm) <= 2:
        return 90 if alias_norm in text_tokens else 0

    if alias_norm in text_norm:
        return 80
    if alias_tokens and alias_tokens.issubset(text_tokens):
        return 70
    if text_tokens and text_tokens.issubset(alias_tokens):
        return 60
    return 0


def _resolve_with_index(raw_key: str, alias_index: Dict[str, List[str]]) -> Optional[str]:
    key_norm = normalize_token(raw_key)
    if not key_norm:
        return None

    best_key: Optional[str] = None
    best_score = 0
    best_alias_len = -1

    for canonical, aliases in alias_index.items():
        for alias in aliases:
            score = _alias_score(key_norm, alias)
            if score <= 0:
                continue
            alias_len = len(alias)
            if score > best_score or (score == best_score and alias_len > best_alias_len):
                best_key = canonical
                best_score = score
                best_alias_len = alias_len

    return best_key


def normalize_term_variants(text: str, preserve_newlines: bool = True) -> str:
    if not text:
        return ""

    out = text
    for pattern, replacement in TERM_PATTERNS:
        out = pattern.sub(replacement, out)

    if preserve_newlines:
        out = re.sub(r"[ \t]+", " ", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()

    out = re.sub(r"\s+", " ", out)
    return out.strip()


def normalize_token(text: str) -> str:
    value = normalize_term_variants(text, preserve_newlines=False).lower()
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = value.replace("đ", "d")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def slugify_token(text: str) -> str:
    token = normalize_token(text)
    return re.sub(r"[^a-z0-9]+", "_", token).strip("_").upper() or "UNKNOWN"


def _rebuild_alias_indexes() -> None:
    global _PROPERTY_ALIAS_INDEX, _TABLE_ALIAS_INDEX
    _PROPERTY_ALIAS_INDEX = {
        canonical: _normalized_aliases([canonical.replace("_", " "), *aliases])
        for canonical, aliases in PROPERTY_ALIASES.items()
    }
    _TABLE_ALIAS_INDEX = {
        canonical: _normalized_aliases([canonical.replace("_", " "), *aliases])
        for canonical, aliases in TABLE_LABEL_ALIASES.items()
    }


_rebuild_alias_indexes()


def resolve_property_key(raw_key: str) -> str:
    matched = _resolve_with_index(raw_key, _PROPERTY_ALIAS_INDEX)
    return matched or normalize_token(raw_key)


def matches_property_key(raw_key: str, canonical_property: str) -> bool:
    normalized = normalize_token(canonical_property).replace(" ", "_")
    resolved = resolve_property_key(raw_key)
    return resolved == canonical_property or resolved == normalized


def resolve_table_label(raw_label: str) -> str:
    matched = _resolve_with_index(raw_label, _TABLE_ALIAS_INDEX)
    return matched or normalize_token(raw_label).replace(" ", "_")


def matches_table_label(raw_label: str, canonical_label: str) -> bool:
    normalized = normalize_token(canonical_label).replace(" ", "_")
    resolved = resolve_table_label(raw_label)
    return resolved == canonical_label or resolved == normalized
