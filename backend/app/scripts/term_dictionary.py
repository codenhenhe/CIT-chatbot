import re
import unicodedata
from typing import List, Tuple

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
    "ma_nganh": ["ma nganh", "major code"],
    "ten_nganh_vi": ["nganh", "ten nganh", "ten chuong trinh tieng viet", "vietnamese major"],
    "ten_nganh_en": ["major", "ten chuong trinh tieng anh", "english major"],
    "ma_chuong_trinh": ["ma chuong trinh", "program code"],
    "khoa": ["khoa", "cohort", "k"],
    "he": ["he", "program type"],
    "ngon_ngu": ["ngon ngu", "language"],
    "tong_tin_chi": ["tong tin chi", "so tin chi", "credits"],
    "thoi_gian_dao_tao": ["thoi gian dao tao", "duration"],
    "thang_diem": ["thang diem", "grading scale"],
    "ma_hp": ["ma hp", "ma hoc phan", "course id"],
    "ten_hoc_phan": ["ten hoc phan", "course title"],
    "ten_hp": ["ten hp", "ten hoc phan"],
}


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
