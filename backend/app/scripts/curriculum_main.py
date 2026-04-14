import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

if __package__ in (None, ""):
    backend_root = Path(__file__).resolve().parents[2]
    if str(backend_root) not in sys.path:
        sys.path.append(str(backend_root))

from app.scripts.curriculum_parser import PDFParser
from app.scripts.curriculum_table import extract_tables, extract_tables_text
from app.scripts.new_xlctdt import (
    NODE_FIELD_SCHEMAS,
    _normalize_node_payload,
    build_etl_node_prompts,
    extract_entities,
    extract_relationships,
    save_json_output,
)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _default_output_json_path(pdf_path: str) -> str:
    backend_root = Path(__file__).resolve().parents[2]
    output_dir = backend_root / "processed_data" / "json"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir / f"{Path(pdf_path).stem}.json")


def _section_to_text(section: Dict[str, Any], include_children: bool = True) -> str:
    chunks: List[str] = []
    title = _clean_text(section.get("title"))
    content = _clean_text(section.get("content"))

    if title and title != "ROOT":
        chunks.append(title)
    if content:
        chunks.append(content)

    if include_children:
        for child in section.get("children", []):
            child_text = _section_to_text(child, include_children=True)
            if child_text:
                chunks.append(child_text)

    return _clean_text("\n".join(chunks))


def _find_major_section(root: Dict[str, Any], major_number: int) -> Optional[Dict[str, Any]]:
    pattern = re.compile(rf"^{major_number}\.\s+")
    for child in root.get("children", []):
        title = _clean_text(child.get("title"))
        if pattern.match(title):
            return child
    return None


def _extract_raw_pdf_text(pdf_path: str, max_pages: int = 2) -> str:
    try:
        import fitz
    except Exception:
        return ""

    chunks: List[str] = []
    try:
        doc = fitz.open(pdf_path)
        try:
            page_count = min(max_pages, len(doc))
            for page_index in range(page_count):
                page_text = doc[page_index].get_text("text").strip()
                if page_text:
                    chunks.append(page_text)
        finally:
            doc.close()
    except Exception:
        return ""

    return "\n".join(chunks).strip()


def _normalize_course_code(value: Any) -> str:
    return re.sub(r"\s+", "", _clean_text(value) or "").upper()


def _split_course_codes(value: Any) -> List[str]:
    text = _clean_text(value) or ""
    if not text:
        return []
    matches = re.findall(r"[A-Z]{1,4}\s*\d{2,4}[A-Z]*", text.upper())
    return [_normalize_course_code(match) for match in matches]


def _extract_course_relations(value: Any, default_rel_type: str) -> List[tuple[str, str]]:
    text = _clean_text(value) or ""
    if not text:
        return []
    results: List[tuple[str, str]] = []
    for chunk in re.split(r",|;|\n", text):
        chunk = _clean_text(chunk)
        if not chunk:
            continue
        match = re.match(r"^([A-Z]{1,4}\s*\d{2,4}[A-Z]*)(?:\(([^)]+)\))?$", chunk.upper())
        if not match:
            continue
        code = _normalize_course_code(match.group(1))
        qualifier = _clean_text(match.group(2)).upper() if match.group(2) else ""
        if qualifier in {"KN", "KHUYEN NGHI", "KHUYENNGHI"}:
            continue
        if qualifier in {"SH", "SHT", "SONG HANH"}:
            results.append((code, "CO_THE_SONG_HANH"))
        elif qualifier in {"TQ", "TIEN QUYET", "TIENQUYET"}:
            results.append((code, "YEU_CAU_TIEN_QUYET"))
        else:
            results.append((code, default_rel_type))
    return results


def _extract_optional_requirement_label(value: Any) -> Optional[str]:
    text = _clean_text(value)
    if not text:
        return None
    if text.lower() in {"x", "kn", "tq", "sh", "sht"}:
        return None
    text = re.sub(r"^\d+\s*", "", text)
    text = re.sub(r"\bTC\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" -;,")
    if text.lower() in {"tự chọn", "tu chon", "x", "kn", "tq", "sh", "sht"}:
        return None
    return text or None


def _extract_group_code(value: Any) -> Optional[str]:
    text = _clean_text(value)
    if not text:
        return None
    text = text.upper()
    match = re.search(r"\b([A-Z]{1,4}\d{0,3})\b", text)
    if match:
        code = match.group(1)
        if code in {"KN", "TQ", "SH"}:
            return None
        return code
    return None


def _looks_like_relevant_content(node_type: str, text: str) -> bool:
    lowered = _clean_text(text).lower()
    if not lowered:
        return False

    keyword_map = {
        "HinhThucDaoTao": ["hình thức", "chính quy", "vừa làm vừa học", "từ xa", "online", "offline"],
        "PhuongThucDaoTao": ["phương thức", "trực tiếp", "trực tuyến", "kết hợp", "blended", "delivery"],
        "MucTieuDaoTao": ["mục tiêu", "objective", "goals", "aim"],
        "ViTriViecLam": ["vị trí việc làm", "cơ hội việc làm", "sau khi tốt nghiệp", "career opportunities", "job opportunities"],
        "ChuanThamKhao": ["tham khảo", "cdio", "iso", "chuẩn", "reference"],
        "KhaNangHocTap": ["khả năng học tập", "lifelong", "học tập", "suốt đời", "continuing"],
        "ChuanDauRa": ["chuẩn đầu ra", "learning outcome", "learning outcomes", "plo", "cdr", "outcome"],
    }
    keywords = keyword_map.get(node_type, [])
    return any(keyword in lowered for keyword in keywords)


def _split_text_by_heading(raw_text: str, heading_patterns: List[str]) -> List[str]:
    lines = [_clean_text(line) for line in raw_text.splitlines()]
    lines = [line for line in lines if line]
    if not lines:
        return []

    compiled = [re.compile(pattern, re.IGNORECASE) for pattern in heading_patterns]
    collected: List[str] = []
    active = False

    for line in lines:
        if any(pattern.search(line) for pattern in compiled):
            active = True
            continue

        if not active:
            continue

        if re.match(r"^[IVX]+\.", line) or re.match(r"^[A-Z](?:\.\d+)*\.?\s+", line):
            break

        if line.isupper() and len(line.split()) <= 12:
            break

        collected.append(line)

    if not collected:
        return []

    text = _clean_text(" ".join(collected))
    return [text] if text else []


def _parse_credit_amount(value: Any) -> Optional[int]:
    text = _clean_text(value)
    if not text:
        return None
    if "+" in text:
        parts = [part for part in re.split(r"\+", text) if part.strip()]
        values = []
        for part in parts:
            try:
                values.append(int(part.strip()))
            except Exception:
                continue
        return sum(values) if values else None
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else None


def _parse_table_section_header(marker: str, credit_value: str) -> Optional[Dict[str, Any]]:
    text = _clean_text(marker)
    if not text:
        return None

    text = re.sub(r"\.\s*\.", ".", text)
    text = re.sub(r"(\.[0-9A-Z]+)\s+\.", r"\1.", text)
    text = re.sub(r"\s+", " ", text).strip()

    if _normalize_course_code(text) in {"STT", "NO", "COURSEID", "MASOHOCPHAN"}:
        return None

    lowered = text.lower()
    if lowered.startswith("hối kiến thức"):
        text = f"K{text}"
        lowered = text.lower()

    is_lettered_header = re.match(r"^[A-Z](?:\.\d+)*\.?\s+", text) is not None
    is_khoi_header = lowered.startswith("khối kiến thức")
    if not is_lettered_header and not is_khoi_header:
        return None

    if is_khoi_header:
        header_code = None
        title_part = text
    else:
        prefix_match = re.match(r"^([A-Z](?:\.\d+)*\.?)(?:\s+)?(.*)$", text)
        header_code = prefix_match.group(1).rstrip(".") if prefix_match else None
        title_part = prefix_match.group(2).strip() if prefix_match else text

    if is_khoi_header:
        if "giáo dục đại cương" in lowered:
            header_code = "K1"
        elif "cơ sở ngành" in lowered:
            header_code = "K2"
        elif "chuyên ngành" in lowered:
            header_code = "K3"

    title_part = re.sub(r"\[[A-Z]+\]", "", title_part).strip()
    title_part = re.sub(r"\s+", " ", title_part).strip(" -;,")
    if not title_part:
        title_part = text

    requirement_label = None
    if re.search(r"tự chọn|elective", text, re.IGNORECASE):
        requirement_label = re.split(r"\(|\[", title_part, maxsplit=1)[0].strip()
        requirement_label = re.sub(r"\s+", " ", requirement_label).strip(" -;,") or title_part

    group_code = None
    group_match = re.search(r"(?:nhóm|group)\s+([A-Z])", text, re.IGNORECASE)
    if group_match:
        group_code = group_match.group(1).upper()

    return {
        "ma_khoi": header_code or title_part,
        "ten_khoi": title_part,
        "tong_tin_chi": _parse_credit_amount(credit_value),
        "tin_chi_bat_buoc": None,
        "tin_chi_tu_chon": None,
        "requirement_label": requirement_label,
        "group_code": group_code,
    }


def _has_meaningful_value(item: Dict[str, Any]) -> bool:
    return any(value not in (None, "", [], {}) for value in item.values())


def _merge_node_items(node_type: str, existing: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    existing_items = existing.get("items", []) if isinstance(existing, dict) else []
    fallback_items = fallback.get("items", []) if isinstance(fallback, dict) else []

    if not fallback_items:
        return existing if isinstance(existing, dict) and existing else {"node_type": node_type, "items": existing_items or []}

    if not existing_items:
        return {"node_type": node_type, "items": fallback_items}

    if not any(_has_meaningful_value(item) for item in existing_items):
        return {"node_type": node_type, "items": fallback_items}

    key_field_map = {
        "KhoiKienThuc": ["ten_khoi", "ma_khoi"],
        "YeuCauTuChon": ["noi_dung_yeu_cau"],
        "NhomHocPhanTuChon": ["ten_nhom"],
        "HocPhan": ["ma_hp"],
    }
    key_fields = key_field_map.get(node_type, [])

    def _item_key(item: Dict[str, Any]) -> str:
        if not key_fields:
            return json.dumps(item, ensure_ascii=False, sort_keys=True)
        return "|".join(_clean_text(item.get(field)) or "" for field in key_fields)

    merged: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}

    for item in existing_items:
        key = _item_key(item)
        seen[key] = len(merged)
        merged.append(item)

    for item in fallback_items:
        key = _item_key(item)
        if key in seen:
            index = seen[key]
            current = merged[index]
            updated = dict(current)
            for field, value in item.items():
                if current.get(field) in (None, "", [], {}) and value not in (None, "", [], {}):
                    updated[field] = value
            merged[index] = updated
        else:
            seen[key] = len(merged)
            merged.append(item)

    return {"node_type": node_type, "items": merged}


def _extract_table_backed_nodes_and_relationships(tables: List[Dict[str, Any]], program_name: str) -> tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    fallback_nodes: Dict[str, Dict[str, Any]] = {
        "KhoiKienThuc": {"node_type": "KhoiKienThuc", "items": []},
        "YeuCauTuChon": {"node_type": "YeuCauTuChon", "items": []},
        "NhomHocPhanTuChon": {"node_type": "NhomHocPhanTuChon", "items": []},
        "HocPhan": {"node_type": "HocPhan", "items": []},
    }
    relationships: List[Dict[str, Any]] = []

    current_block: Optional[Dict[str, Any]] = None
    current_requirement: Optional[Dict[str, Any]] = None
    current_group_code: Optional[str] = None
    seen_course_codes: set[str] = set()

    def add_relationship(source_type: str, source_match: str, rel_type: str, target_type: str, target_match: str, evidence: str) -> None:
        relationships.append(
            {
                "source_node_type": source_type,
                "source_match": source_match,
                "rel_type": rel_type,
                "target_node_type": target_type,
                "target_match": target_match,
                "evidence": evidence,
            }
        )

    for table in tables:
        rows = table.get("rows", []) or []
        for row in rows:
            if not row:
                continue

            marker = _clean_text(row[0] if len(row) > 0 else None)
            course_code = _normalize_course_code(row[1] if len(row) > 1 else None)
            course_name = _clean_text(row[2] if len(row) > 2 else None)
            credit_value = _clean_text(row[3] if len(row) > 3 else None)
            mandatory_value = _clean_text(row[4] if len(row) > 4 else None)
            optional_value = _clean_text(row[5] if len(row) > 5 else None)
            lt_value = _clean_text(row[6] if len(row) > 6 else None)
            th_value = _clean_text(row[7] if len(row) > 7 else None)
            prerequisite_value = _clean_text(row[8] if len(row) > 8 else None)
            concurrent_value = _clean_text(row[9] if len(row) > 9 else None)

            header_info = _parse_table_section_header(marker or "", credit_value or "")
            if header_info is not None and not course_code:
                current_block = {
                    "ma_khoi": header_info["ma_khoi"],
                    "ten_khoi": header_info["ten_khoi"],
                    "tong_tin_chi": header_info["tong_tin_chi"],
                    "tin_chi_bat_buoc": header_info["tong_tin_chi"] if header_info["requirement_label"] else header_info["tong_tin_chi"],
                    "tin_chi_tu_chon": 0 if header_info["requirement_label"] else None,
                }
                fallback_nodes["KhoiKienThuc"]["items"].append(current_block)
                current_requirement = None
                current_group_code = None
                add_relationship(
                    "ChuongTrinhDaoTao",
                    program_name,
                    "CO_KHOI_KIEN_THUC",
                    "KhoiKienThuc",
                    current_block["ten_khoi"],
                    marker,
                )
                if header_info["requirement_label"]:
                    current_requirement = {
                        "noi_dung_yeu_cau": header_info["requirement_label"],
                        "so_tin_chi_yeu_cau": header_info["tong_tin_chi"],
                    }
                    fallback_nodes["YeuCauTuChon"]["items"].append(current_requirement)
                    add_relationship(
                        "KhoiKienThuc",
                        current_block["ten_khoi"],
                        "CO_YEU_CAU_TU_CHON",
                        "YeuCauTuChon",
                        header_info["requirement_label"],
                        marker,
                    )
                if header_info["group_code"]:
                    current_group_code = header_info["group_code"]
                    if current_group_code not in {item.get("ten_nhom") for item in fallback_nodes["NhomHocPhanTuChon"]["items"]}:
                        fallback_nodes["NhomHocPhanTuChon"]["items"].append({"ten_nhom": current_group_code})
                    if current_requirement:
                        add_relationship(
                            "YeuCauTuChon",
                            current_requirement["noi_dung_yeu_cau"],
                            "CO_NHOM_THANH_PHAN",
                            "NhomHocPhanTuChon",
                            current_group_code,
                            marker,
                        )
                continue

            if marker and marker.lower().startswith("cộng") and current_block:
                total_match = re.search(r"(\d+)\s*tc", marker, re.IGNORECASE)
                mandatory_match = re.search(r"bắt\s*buộc\s*[:] ?\s*(\d+)\s*tc", marker, re.IGNORECASE)
                optional_match = re.search(r"tự\s*chọn\s*[:] ?\s*(\d+)\s*tc", marker, re.IGNORECASE)
                current_block["tong_tin_chi"] = int(total_match.group(1)) if total_match else current_block["tong_tin_chi"]
                current_block["tin_chi_bat_buoc"] = int(mandatory_match.group(1)) if mandatory_match else current_block["tin_chi_bat_buoc"]
                current_block["tin_chi_tu_chon"] = int(optional_match.group(1)) if optional_match else current_block["tin_chi_tu_chon"]
                continue

            if mandatory_value and mandatory_value.isdigit():
                current_requirement = None
                current_group_code = None

            if current_block and optional_value:
                requirement_label = _extract_optional_requirement_label(optional_value)
                if requirement_label:
                    credit_requirement = re.search(r"\d+", optional_value)
                    current_requirement = {
                        "noi_dung_yeu_cau": requirement_label,
                        "so_tin_chi_yeu_cau": credit_requirement.group(0) if credit_requirement else None,
                    }
                    fallback_nodes["YeuCauTuChon"]["items"].append(current_requirement)
                    add_relationship(
                        "KhoiKienThuc",
                        current_block["ten_khoi"],
                        "CO_YEU_CAU_TU_CHON",
                        "YeuCauTuChon",
                        requirement_label,
                        optional_value,
                    )

            if not course_code or course_code in {"MÃSỐHỌCPHẦN", "MASOHOCPHAN"} or (course_name and "tên học phần" in course_name.lower()):
                continue

            if not course_code or not course_name:
                continue

            if course_code in seen_course_codes:
                continue
            seen_course_codes.add(course_code)

            if current_block:
                add_relationship(
                    "ChuongTrinhDaoTao",
                    program_name,
                    "CO_KHOI_KIEN_THUC",
                    "KhoiKienThuc",
                    current_block["ten_khoi"],
                    current_block["ten_khoi"],
                )
                add_relationship(
                    "KhoiKienThuc",
                    current_block["ten_khoi"],
                    "GOM_HOC_PHAN",
                    "HocPhan",
                    course_code,
                    course_name,
                )

            if current_requirement:
                add_relationship(
                    "YeuCauTuChon",
                    current_requirement["noi_dung_yeu_cau"],
                    "GOM_HOC_PHAN",
                    "HocPhan",
                    course_code,
                    course_name,
                )
                if current_group_code:
                    add_relationship(
                        "NhomHocPhanTuChon",
                        current_group_code,
                        "GOM_HOC_PHAN",
                        "HocPhan",
                        course_code,
                        course_name,
                    )

            fallback_nodes["HocPhan"]["items"].append(
                {
                    "ma_hp": course_code,
                    "ten_hp": course_name,
                    "so_tin_chi": _parse_credit_amount(credit_value),
                    "so_tiet_ly_thuyet": _parse_credit_amount(lt_value),
                    "so_tiet_thuc_hanh": _parse_credit_amount(th_value),
                    "bat_buoc": True if mandatory_value and mandatory_value.isdigit() else False if mandatory_value else None,
                }
            )

            if prerequisite_value:
                for prereq_code, rel_type in _extract_course_relations(prerequisite_value, "YEU_CAU_TIEN_QUYET"):
                    add_relationship(
                        "HocPhan",
                        course_code,
                        rel_type,
                        "HocPhan",
                        prereq_code,
                        prerequisite_value,
                    )

            if concurrent_value:
                for concurrent_code, rel_type in _extract_course_relations(concurrent_value, "CO_THE_SONG_HANH"):
                    add_relationship(
                        "HocPhan",
                        course_code,
                        rel_type,
                        "HocPhan",
                        concurrent_code,
                        concurrent_value,
                    )

    return fallback_nodes, relationships


def _dedupe_relationships(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for item in items:
        key = (
            _clean_text(item.get("source_node_type")),
            _clean_text(item.get("source_match")),
            _clean_text(item.get("rel_type")),
            _clean_text(item.get("target_node_type")),
            _clean_text(item.get("target_match")),
            _clean_text(item.get("evidence")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_front_matter_nodes(raw_text: str) -> tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    fallback_nodes: Dict[str, Dict[str, Any]] = {
        "VanBanPhapLy": {"node_type": "VanBanPhapLy", "items": []},
        "TrinhDo": {"node_type": "TrinhDo", "items": []},
        "Nganh": {"node_type": "Nganh", "items": []},
        "Khoa": {"node_type": "Khoa", "items": []},
        "LoaiVanBang": {"node_type": "LoaiVanBang", "items": []},
        "ChuongTrinhDaoTao": {"node_type": "ChuongTrinhDaoTao", "items": []},
    }
    relationships: List[Dict[str, Any]] = []
    text = _clean_text(raw_text)
    if not text:
        return fallback_nodes, relationships

    lines = [_clean_text(line) for line in raw_text.splitlines()]
    lines = [line for line in lines if line]

    def _find_line(prefix: str) -> Optional[str]:
        prefix_lower = prefix.lower()
        for line in lines:
            if line.lower().startswith(prefix_lower):
                return line
        return None

    title_line = _find_line("CHƯƠNG TRÌNH ĐÀO TẠO") or _find_line("BACHELOR PROGRAM")
    degree_text = None
    if title_line:
        if re.search(r"TRÌNH\s*ĐỘ\s*ĐẠI\s*HỌC", title_line, re.IGNORECASE):
            degree_text = "Đại học"
        if re.search(r"CỬ\s*NHÂN", title_line, re.IGNORECASE):
            degree_text = "Cử nhân"
        elif re.search(r"THẠC\s*SĨ", title_line, re.IGNORECASE):
            degree_text = "Thạc sĩ"
        elif re.search(r"TIẾN\s*SĨ", title_line, re.IGNORECASE):
            degree_text = "Tiến sĩ"

    if not degree_text and re.search(r"trình\s*độ\s*đại\s*học", text, re.IGNORECASE):
        degree_text = "Đại học"

    if degree_text:
        fallback_nodes["TrinhDo"]["items"].append({"ten_trinh_do": degree_text.title() if degree_text.isupper() else degree_text})

    cohort_match = re.search(r"KHÓA\s+(\d{4})", text, re.IGNORECASE)
    major_code_match = re.search(r"mã\s*ngành\s*[:\-]?\s*(\d{4,10})", text, re.IGNORECASE)
    total_credit_match = re.search(r"(?:tổng\s*số\s*tín\s*chỉ|số\s*tín\s*chỉ)\s*[:\-]?\s*(\d{2,3})", text, re.IGNORECASE)
    he_match = re.search(r"(chất\s*lượng\s*cao|clc|đại\s*trà)", text, re.IGNORECASE)
    language_match = re.search(r"ngôn\s*ngữ\s*(?:đào\s*tạo|giảng\s*dạy)?\s*[:\-]?\s*(tiếng\s*anh|tiếng\s*việt)", text, re.IGNORECASE)
    degree_line = next((line for line in lines if "loại văn bằng" in line.lower()), None)
    faculty_line = _find_line("KHOA:")
    if not faculty_line:
        faculty_line = _find_line("Đơn vị quản lý:") or _find_line("Don vi quan ly:")
    major_vi_line = _find_line("Ngành:")
    major_en_line = _find_line("Major:")
    decision_line = next(
        (line for line in lines if "quyết định số" in line.lower() and "ngày" in line.lower()),
        None,
    )

    khoa_candidates: List[str] = []
    if faculty_line:
        faculty_value = _clean_text(faculty_line.split(":", 1)[1]) if ":" in faculty_line else _clean_text(faculty_line)
        if faculty_value:
            khoa_candidates.append(faculty_value)

    # Bổ sung bắt theo ngữ cảnh rõ ràng ở phần đầu tài liệu, tránh hút nhầm "Khoa học ..." trong bảng học phần.
    for pattern in [
        r"(?im)^\s*(?:đơn\s*vị\s*quản\s*lý|don\s*vi\s*quan\s*ly)\s*[:\-]?\s*([^\n;]+)",
        r"(?im)^\s*KHOA\s*[:\-]\s*([^\n;]+)",
        r"(?im)^\s*FACULTY\s*(?:OF)?\s*[:\-]?\s*([^\n;]+)",
    ]:
        for match in re.finditer(pattern, raw_text, re.IGNORECASE):
            value = _clean_text(match.group(1))
            if value:
                khoa_candidates.append(value)

    seen_khoa = set()
    for candidate in khoa_candidates:
        normalized = re.sub(r"\s+", " ", candidate).strip(" .;,")
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen_khoa:
            continue
        seen_khoa.add(key)
        fallback_nodes["Khoa"]["items"].append({"ten_khoa": normalized})

    major_vi = _clean_text(major_vi_line.split(":", 1)[1]) if major_vi_line and ":" in major_vi_line else None
    major_en = _clean_text(major_en_line.split(":", 1)[1]) if major_en_line and ":" in major_en_line else None
    major_vi_clean = re.sub(r"\s*-\s*[\d.]+\s*Tín chỉ.*$", "", major_vi or "").strip() or None
    major_en_clean = re.sub(r"\s*-\s*[\d.]+\s*Credits.*$", "", major_en or "").strip() or None
    if major_vi_clean and not major_en_clean:
        pair_match = re.match(r"^(.*?)\s*\(([^()]+)\)\s*$", major_vi_clean)
        if pair_match:
            major_vi_clean = _clean_text(pair_match.group(1))
            major_en_clean = _clean_text(pair_match.group(2))

    duration_match = re.search(r"thời\s*gian\s*đào\s*tạo\s*[:\-]?\s*([\d,.]+)\s*năm", text, re.IGNORECASE)
    duration_years = None
    if duration_match:
        try:
            duration_years = float(duration_match.group(1).replace(",", "."))
        except Exception:
            duration_years = None

    if major_vi or major_en:
        fallback_nodes["Nganh"]["items"].append(
            {
                "ma_nganh": _clean_text(major_code_match.group(1)) if major_code_match else None,
                "ten_nganh_vi": major_vi_clean,
                "ten_nganh_en": major_en_clean,
            }
        )

    if degree_text:
        fallback_nodes["LoaiVanBang"]["items"].append({"loai_van_bang": degree_text.title() if degree_text.isupper() else degree_text})
    elif degree_line:
        degree_value = _clean_text(degree_line.split(":", 1)[1]) if ":" in degree_line else _clean_text(degree_line)
        if degree_value:
            fallback_nodes["LoaiVanBang"]["items"].append({"loai_van_bang": degree_value})

    ctdt_he = "Đại trà"
    if he_match:
        he_text = _clean_text(he_match.group(1)).lower()
        if "chất lượng cao" in he_text or "clc" in he_text:
            ctdt_he = "Chất lượng cao"

    ctdt_ngon_ngu = "Tiếng Anh" if ctdt_he == "Chất lượng cao" else "Tiếng Việt"
    if language_match:
        ctdt_ngon_ngu = "Tiếng Anh" if "anh" in language_match.group(1).lower() else "Tiếng Việt"

    program_item = {
        "ma_chuong_trinh": _clean_text(major_code_match.group(1)) if major_code_match else None,
        "khoa": int(cohort_match.group(1)) if cohort_match else None,
        "he": ctdt_he,
        "ngon_ngu": ctdt_ngon_ngu,
        "tong_tin_chi": int(total_credit_match.group(1)) if total_credit_match else None,
        "thoi_gian_dao_tao": duration_years,
    }
    fallback_nodes["ChuongTrinhDaoTao"]["items"].append(program_item)

    if decision_line:
        decision_match = re.search(r"quyết định số\s*([\w/.-]+).*?ngày\s*(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})", decision_line, re.IGNORECASE)
        so = _clean_text(decision_match.group(1)) if decision_match else None
        ngay = f"{decision_match.group(2).zfill(2)}/{decision_match.group(3).zfill(2)}/{decision_match.group(4)}" if decision_match else None
        fallback_nodes["VanBanPhapLy"]["items"].append(
            {
                "so": so,
                "ten": f"Quyết định số {so} ngày {ngay}" if so and ngay else _clean_text(decision_line),
                "loai": "Quyết định",
                "ngay_ban_hanh": ngay,
                "co_quan_ban_hanh": _clean_text(faculty_line.split(":", 1)[1]) if faculty_line and ":" in faculty_line else None,
                "noi_dung_goc": decision_line,
            }
        )

    if fallback_nodes["ChuongTrinhDaoTao"]["items"] and fallback_nodes["TrinhDo"]["items"]:
        relationships.append(
            {
                "source_node_type": "ChuongTrinhDaoTao",
                "source_match": text[:120],
                "rel_type": "DAO_TAO_TRINH_DO",
                "target_node_type": "TrinhDo",
                "target_match": fallback_nodes["TrinhDo"]["items"][0].get("ten_trinh_do"),
                "evidence": text[:120],
            }
        )

    return fallback_nodes, relationships


def _extract_content_nodes(raw_text: str) -> tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    fallback_nodes: Dict[str, Dict[str, Any]] = {
        "MucTieuDaoTao": {"node_type": "MucTieuDaoTao", "items": []},
        "ViTriViecLam": {"node_type": "ViTriViecLam", "items": []},
        "ChuanThamKhao": {"node_type": "ChuanThamKhao", "items": []},
        "KhaNangHocTap": {"node_type": "KhaNangHocTap", "items": []},
        "ChuanDauRa": {"node_type": "ChuanDauRa", "items": []},
    }
    relationships: List[Dict[str, Any]] = []
    text = _clean_text(raw_text)
    if not text:
        return fallback_nodes, relationships

    section_specs = {
        "MucTieuDaoTao": [r"^(?:\d+(?:\.\d+)*\.?\s*)?mục tiêu đào tạo\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?program objectives\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?educational objectives\b"],
        "ViTriViecLam": [r"^(?:\d+(?:\.\d+)*\.?\s*)?vị trí việc làm\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?cơ hội việc làm\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?career opportunities\b"],
        "ChuanThamKhao": [r"^(?:\d+(?:\.\d+)*\.?\s*)?chuẩn tham khảo\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?references\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?tham khảo\b"],
        "KhaNangHocTap": [r"^(?:\d+(?:\.\d+)*\.?\s*)?khả năng học tập\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?lifelong learning\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?học tập suốt đời\b"],
        "ChuanDauRa": [r"^(?:\d+(?:\.\d+)*\.?\s*)?chuẩn đầu ra\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?learning outcomes\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?program learning outcomes\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?plo\b", r"^(?:\d+(?:\.\d+)*\.?\s*)?cdr\b"],
    }

    for node_type, patterns in section_specs.items():
        chunks = _split_text_by_heading(raw_text, patterns)
        if not chunks:
            continue

        content = chunks[0]
        if node_type == "ChuanDauRa":
            normalized = re.sub(r"\s+", " ", content)
            segments = [seg.strip() for seg in re.split(r";", normalized) if seg.strip()]
            for seg in segments:
                code_match = re.search(r"\((PLO\d+|CDR[\w.-]*|CĐR[\w.-]*)\)", seg, re.IGNORECASE)
                if not code_match:
                    continue
                code = _clean_text(code_match.group(1)).upper()
                statement = _clean_text(re.sub(r"\((PLO\d+|CDR[\w.-]*|CĐR[\w.-]*)\)", "", seg, flags=re.IGNORECASE))
                statement = re.sub(r"^\d+(?:\.\d+)*\s*", "", statement).strip()
                statement = re.sub(r"^[a-zA-Z]\.?\s*", "", statement).strip()
                if not statement or statement in {")", "(", "-"}:
                    continue
                fallback_nodes[node_type]["items"].append(
                    {
                        "ma_chuan": code,
                        "nhom": None,
                        "loai": "chi_tiet",
                        "noi_dung": statement,
                    }
                )
        elif node_type == "MucTieuDaoTao":
            parts = re.split(r"(?:;|\s•\s|\n\d+\s*[-.)]\s+)", content)
            parts = [_clean_text(part) for part in parts if _clean_text(part)]
            if parts:
                for part in parts:
                    if len(part.split()) < 4:
                        continue
                    fallback_nodes[node_type]["items"].append({"loai": "chung", "noi_dung": part})
            else:
                fallback_nodes[node_type]["items"].append({"loai": "chung", "noi_dung": content})
        elif node_type == "ViTriViecLam":
            parts = re.split(r"(?:\s+-\s+|;|\n|\d+\s*[-.)]\s+)", content)
            parts = [_clean_text(part) for part in parts if _clean_text(part)]
            if parts:
                for part in parts:
                    if len(part.split()) < 2:
                        continue
                    part = part.lstrip("- ").strip()
                    if part:
                        fallback_nodes[node_type]["items"].append({"noi_dung": part})
            else:
                fallback_nodes[node_type]["items"].append({"noi_dung": content})
        elif node_type == "ChuanThamKhao":
            normalized = _clean_text(content)
            if normalized:
                parts = [
                    p for p in re.split(
                        r"\s+-\s+|\n\s*[-+•]\s+|\s*;\s*(?=(?:-|quyết định|thông tư|nghị định|nghị quyết|luật|aun|abet|cdio|acm|ieee|https?://|www\.))",
                        normalized,
                        flags=re.IGNORECASE,
                    ) if _clean_text(p)
                ]
                if not parts:
                    parts = [normalized]

                seen = set()
                for part in parts:
                    part = _clean_text(part).lstrip("- ").strip(" .;")
                    if len(part.split()) < 3:
                        continue
                    part = re.sub(r"\s+", " ", part)
                    key = part.lower()
                    if key in seen:
                        continue
                    seen.add(key)

                    urls = re.findall(r"(?:https?://\S+|www\.\S+)", part, flags=re.IGNORECASE)
                    url = urls[0].rstrip(".,;)") if urls else None
                    if url and url.lower().startswith("www."):
                        url = f"https://{url}"

                    fallback_nodes[node_type]["items"].append(
                        {
                            "noi_dung": part,
                            "link": url,
                            "noi_dung_goc": part,
                        }
                    )
        elif node_type == "KhaNangHocTap":
            parts = re.split(r"(?:\s+-\s+|;|\n)", content)
            parts = [_clean_text(part) for part in parts if _clean_text(part)]
            if parts:
                for part in parts:
                    if len(part.split()) < 4:
                        continue
                    fallback_nodes[node_type]["items"].append({"noi_dung": part.lstrip("- ").strip()})
            else:
                fallback_nodes[node_type]["items"].append({"noi_dung": content})

    if fallback_nodes["ChuanDauRa"]["items"]:
        unique = []
        seen = set()
        for item in fallback_nodes["ChuanDauRa"]["items"]:
            key = (_clean_text(item.get("ma_chuan")), _clean_text(item.get("noi_dung")))
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        fallback_nodes["ChuanDauRa"]["items"] = unique

    return fallback_nodes, relationships


def _build_node_contexts(sections: Dict[str, Any], tables_text: str, raw_text: str = "") -> Dict[str, str]:
    intro_chunks: List[str] = []
    children = sections.get("children", [])
    if children:
        intro_chunks.append(_section_to_text(children[0], include_children=True))

    sec1 = _find_major_section(sections, 1)
    sec2 = _find_major_section(sections, 2)
    sec3 = _find_major_section(sections, 3)
    sec4 = _find_major_section(sections, 4)
    sec5 = _find_major_section(sections, 5)
    sec6 = _find_major_section(sections, 6)
    sec7 = _find_major_section(sections, 7)

    if sec1:
        intro_chunks.append(_section_to_text(sec1, include_children=True))

    general_text = _clean_text("\n".join(intro_chunks))
    sec2_text = _section_to_text(sec2, include_children=True) if sec2 else ""
    sec3_text = _section_to_text(sec3, include_children=True) if sec3 else ""
    sec4_text = _section_to_text(sec4, include_children=True) if sec4 else ""
    sec5_text = _section_to_text(sec5, include_children=True) if sec5 else ""
    sec6_text = _section_to_text(sec6, include_children=True) if sec6 else ""
    sec7_text = _section_to_text(sec7, include_children=True) if sec7 else ""

    curriculum_text = _clean_text("\n".join([sec7_text, tables_text]))

    if len(general_text) < 120 and raw_text:
        general_text = raw_text

    contexts: Dict[str, str] = {
        "VanBanPhapLy": _clean_text("\n".join([general_text, sec6_text])),
        "TrinhDo": general_text,
        "Nganh": general_text,
        "Khoa": general_text,
        "BoMon": general_text,
        "ChuongTrinhDaoTao": general_text,
        "LoaiVanBang": general_text,
        "HinhThucDaoTao": general_text,
        "PhuongThucDaoTao": general_text,
        "MucTieuDaoTao": sec2_text,
        "ViTriViecLam": sec4_text,
        "ChuanThamKhao": sec6_text,
        "KhaNangHocTap": sec5_text,
        "ChuanDauRa": sec3_text,
        "KhoiKienThuc": curriculum_text,
        "YeuCauTuChon": curriculum_text,
        "NhomHocPhanTuChon": curriculum_text,
        "HocPhan": curriculum_text,
    }

    # Fallback to global content if some section is missing in PDF.
    global_text = _section_to_text(sections, include_children=True)
    if len(global_text) < 120 and raw_text:
        global_text = raw_text
    for node_type, text in contexts.items():
        if not _clean_text(text):
            contexts[node_type] = global_text

    return contexts


def _merge_fallback_maps(*maps: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for node_map in maps:
        for node_type, payload in node_map.items():
            merged[node_type] = _merge_node_items(
                node_type,
                merged.get(node_type, {"node_type": node_type, "items": []}),
                payload,
            )
    return merged


def _extract_context_list_nodes(node_contexts: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    fallback_nodes: Dict[str, Dict[str, Any]] = {
        "ViTriViecLam": {"node_type": "ViTriViecLam", "items": []},
        "KhaNangHocTap": {"node_type": "KhaNangHocTap", "items": []},
    }

    vi_tri_text = _clean_text(node_contexts.get("ViTriViecLam"))
    has_vi_tri_heading = bool(re.search(r"\b(vị\s*trí\s*việc\s*làm|cơ\s*hội\s*việc\s*làm|career\s+opportunities)\b", vi_tri_text, re.IGNORECASE))
    if not has_vi_tri_heading:
        vi_tri_text = ""
    if vi_tri_text:
        body = re.sub(r"^\d+(?:\.\d+)*\.?\s*Vị trí việc làm của người tốt nghiệp\s*", "", vi_tri_text, flags=re.IGNORECASE)
        parts = re.split(r"\s+-\s+", body)
        seen = set()
        for part in parts:
            part = _clean_text(part).lstrip("- ").strip(" .;")
            if len(part.split()) < 4:
                continue
            dot_chunks = [chunk.strip() for chunk in part.split(".") if chunk.strip()]
            if len(dot_chunks) >= 2 and len(set(dot_chunks)) == 1:
                part = dot_chunks[0]
            part = re.sub(r"\s+", " ", part)
            key = part.lower()
            if key in seen:
                continue
            seen.add(key)
            fallback_nodes["ViTriViecLam"]["items"].append({"noi_dung": part})

    kha_nang_text = _clean_text(node_contexts.get("KhaNangHocTap"))
    has_kha_nang_heading = bool(re.search(r"\b(khả\s*năng\s*học\s*tập|học\s*tập\s*suốt\s*đời|lifelong\s+learning)\b", kha_nang_text, re.IGNORECASE))
    if not has_kha_nang_heading:
        kha_nang_text = ""
    if kha_nang_text:
        body = re.sub(r"^\d+(?:\.\d+)*\.?\s*Khả năng học tập, nâng cao trình độ sau khi tốt nghiệp\s*", "", kha_nang_text, flags=re.IGNORECASE)
        parts = re.split(r"\s+-\s+", body)
        seen = set()
        for part in parts:
            part = _clean_text(part).lstrip("- ").strip(" .;")
            if len(part.split()) < 5:
                continue
            part = re.sub(r"\s+", " ", part)
            key = part.lower()
            if key in seen:
                continue
            seen.add(key)
            fallback_nodes["KhaNangHocTap"]["items"].append({"noi_dung": part})

    return fallback_nodes


def _prune_empty_items(node_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(node_payload, dict):
        return node_payload

    items = node_payload.get("items")
    if not isinstance(items, list):
        return node_payload

    filtered = [item for item in items if isinstance(item, dict) and _has_meaningful_value(item)]
    node_payload["items"] = filtered
    return node_payload


def _normalize_he_code(he: str) -> str:
    value = _clean_text(he).lower()
    if "chất lượng cao" in value or value == "clc":
        return "CLC"
    return "STD"


def _to_int_safe(value: Any) -> Optional[int]:
    text = _clean_text(value)
    if not text:
        return None
    match = re.search(r"\d+", text)
    if not match:
        return None
    return int(match.group(0))


def _finalize_curriculum_nodes(results: Dict[str, Dict[str, Any]], raw_text: str) -> None:
    nganh_items = results.get("Nganh", {}).get("items", []) if isinstance(results.get("Nganh"), dict) else []
    ctdt_items = results.get("ChuongTrinhDaoTao", {}).get("items", []) if isinstance(results.get("ChuongTrinhDaoTao"), dict) else []
    vanban_items = results.get("VanBanPhapLy", {}).get("items", []) if isinstance(results.get("VanBanPhapLy"), dict) else []
    khoa_items = results.get("Khoa", {}).get("items", []) if isinstance(results.get("Khoa"), dict) else []
    chuan_tham_khao_items = results.get("ChuanThamKhao", {}).get("items", []) if isinstance(results.get("ChuanThamKhao"), dict) else []
    vi_tri_items = results.get("ViTriViecLam", {}).get("items", []) if isinstance(results.get("ViTriViecLam"), dict) else []
    kha_nang_items = results.get("KhaNangHocTap", {}).get("items", []) if isinstance(results.get("KhaNangHocTap"), dict) else []

    major_code = None
    if nganh_items and isinstance(nganh_items[0], dict):
        major_code = _clean_text(nganh_items[0].get("ma_nganh"))
    if not major_code:
        major_match = re.search(r"mã\s*ngành\s*[:\-]?\s*(\d{4,10})", raw_text, re.IGNORECASE)
        major_code = _clean_text(major_match.group(1)) if major_match else None

    if nganh_items and isinstance(nganh_items[0], dict) and major_code and not nganh_items[0].get("ma_nganh"):
        nganh_items[0]["ma_nganh"] = major_code

    if ctdt_items and isinstance(ctdt_items[0], dict):
        ctdt = ctdt_items[0]
        if major_code and not ctdt.get("ma_chuong_trinh"):
            ctdt["ma_chuong_trinh"] = major_code

        if not ctdt.get("tong_tin_chi"):
            total_match = re.search(r"(?:tổng\s*số\s*tín\s*chỉ|số\s*tín\s*chỉ)\s*[:\-]?\s*(\d{2,3})", raw_text, re.IGNORECASE)
            if total_match:
                ctdt["tong_tin_chi"] = int(total_match.group(1))

        if not ctdt.get("he"):
            ctdt["he"] = "Chất lượng cao" if re.search(r"chất\s*lượng\s*cao|\bclc\b", raw_text, re.IGNORECASE) else "Đại trà"

        if not ctdt.get("ngon_ngu"):
            ctdt["ngon_ngu"] = "Tiếng Anh" if _normalize_he_code(ctdt.get("he")) == "CLC" else "Tiếng Việt"

        if not ctdt.get("thoi_gian_dao_tao"):
            duration_match = re.search(r"thời\s*gian\s*đào\s*tạo\s*[:\-]?\s*([\d,.]+)\s*năm", raw_text, re.IGNORECASE)
            if duration_match:
                try:
                    ctdt["thoi_gian_dao_tao"] = float(duration_match.group(1).replace(",", "."))
                except Exception:
                    pass

        # CTU-specific cohort rule: khoa = year_ban_hanh - 1974
        if (not ctdt.get("khoa")) and re.search(r"đhct|đại học cần thơ", raw_text, re.IGNORECASE):
            decision_year = None
            for item in vanban_items:
                if isinstance(item, dict):
                    ngay = _clean_text(item.get("ngay_ban_hanh"))
                    if ngay and re.search(r"\d{4}$", ngay):
                        decision_year = int(re.search(r"(\d{4})$", ngay).group(1))
                        break
            if not decision_year:
                year_match = re.search(r"quyết định số[\s\S]{0,120}?năm\s*(\d{4})", raw_text, re.IGNORECASE)
                if year_match:
                    decision_year = int(year_match.group(1))
            if decision_year:
                ctdt["khoa"] = decision_year - 1974

        if "loai_hinh" in ctdt:
            ctdt.pop("loai_hinh", None)

    he_code = _normalize_he_code(ctdt_items[0].get("he")) if ctdt_items and isinstance(ctdt_items[0], dict) else "STD"
    if ctdt_items and isinstance(ctdt_items[0], dict):
        ctdt = ctdt_items[0]
        cohort = _to_int_safe(ctdt.get("khoa"))
        if major_code and cohort is not None:
            ctdt["ma_chuong_trinh"] = f"{major_code}_{cohort}_{he_code}"
        elif major_code and not ctdt.get("ma_chuong_trinh"):
            ctdt["ma_chuong_trinh"] = major_code

    khoi_payload = results.get("KhoiKienThuc")
    khoi_total_sum = 0
    khoi_total_count = 0
    if isinstance(khoi_payload, dict) and isinstance(khoi_payload.get("items"), list):
        for item in khoi_payload["items"]:
            if not isinstance(item, dict):
                continue

            tong = _to_int_safe(item.get("tong_tin_chi"))
            bat_buoc = _to_int_safe(item.get("tin_chi_bat_buoc"))
            tu_chon = _to_int_safe(item.get("tin_chi_tu_chon"))

            if tong is not None and bat_buoc is None and tu_chon is not None:
                bat_buoc = max(tong - tu_chon, 0)
            elif tong is not None and tu_chon is None and bat_buoc is not None:
                tu_chon = max(tong - bat_buoc, 0)
            elif tong is not None and bat_buoc is None and tu_chon is None:
                bat_buoc = tong
                tu_chon = 0

            if tong is not None:
                item["tong_tin_chi"] = tong
                khoi_total_sum += tong
                khoi_total_count += 1
            if bat_buoc is not None:
                item["tin_chi_bat_buoc"] = bat_buoc
            if tu_chon is not None:
                item["tin_chi_tu_chon"] = tu_chon

            suffix = None
            ma_khoi = _clean_text(item.get("ma_khoi"))
            if ma_khoi:
                match = re.search(r"(K\d+)$", ma_khoi.upper())
                if match:
                    suffix = match.group(1)
            if not suffix:
                title = _clean_text(item.get("ten_khoi")).lower()
                if "giáo dục đại cương" in title:
                    suffix = "K1"
                elif "cơ sở ngành" in title:
                    suffix = "K2"
                elif "chuyên ngành" in title:
                    suffix = "K3"

            if major_code and suffix:
                item["ma_khoi"] = f"{major_code}_{he_code}_{suffix}"

    if ctdt_items and isinstance(ctdt_items[0], dict):
        ctdt = ctdt_items[0]
        if not ctdt.get("tong_tin_chi") and khoi_total_count:
            ctdt["tong_tin_chi"] = khoi_total_sum

    major_name_norm = ""
    if nganh_items and isinstance(nganh_items[0], dict):
        major_name_norm = _clean_text(nganh_items[0].get("ten_nganh_vi")).lower()

    if isinstance(khoa_items, list) and khoa_items:
        filtered_khoa: List[Dict[str, Any]] = []
        seen_khoa = set()
        for item in khoa_items:
            if not isinstance(item, dict):
                continue
            ten_khoa = _clean_text(item.get("ten_khoa"))
            if not ten_khoa:
                continue
            ten_khoa = ten_khoa.strip(" .;,)")
            if not re.match(r"^(khoa|trường)\s+", ten_khoa, flags=re.IGNORECASE):
                continue
            if re.search(r"\d|tín\s*chỉ|credits?|course|specialty|major|\[", ten_khoa, re.IGNORECASE):
                continue
            lowered = ten_khoa.lower()
            if lowered in {"khoa học", "khoa học chính trị", "trường chuyên nghiệp"}:
                continue
            if major_name_norm and lowered == major_name_norm:
                continue
            if major_name_norm and lowered.startswith(major_name_norm + " -"):
                continue
            key = lowered
            if key in seen_khoa:
                continue
            seen_khoa.add(key)
            filtered_khoa.append({"ten_khoa": ten_khoa})
        if filtered_khoa:
            results["Khoa"]["items"] = filtered_khoa

    def _is_noisy_text_item(text: str) -> bool:
        if not text:
            return True
        if len(text) < 20:
            return True
        if text.count("|") >= 2:
            return True
        if re.search(r"\b(stt|course id|course title|credits|học phần|tiên quyết|song hành)\b", text, re.IGNORECASE):
            return True
        if re.search(r"\b(program structure|speciality|major:)\b", text, re.IGNORECASE):
            return True
        return False

    if isinstance(vi_tri_items, list) and vi_tri_items:
        clean_vi_tri: List[Dict[str, Any]] = []
        seen_vi_tri = set()
        for item in vi_tri_items:
            if not isinstance(item, dict):
                continue
            text = _clean_text(item.get("noi_dung"))
            if _is_noisy_text_item(text):
                continue
            key = text.lower()
            if key in seen_vi_tri:
                continue
            seen_vi_tri.add(key)
            clean_vi_tri.append({"noi_dung": text})
        results["ViTriViecLam"]["items"] = clean_vi_tri

    if isinstance(kha_nang_items, list) and kha_nang_items:
        clean_kha_nang: List[Dict[str, Any]] = []
        seen_kha_nang = set()
        for item in kha_nang_items:
            if not isinstance(item, dict):
                continue
            text = _clean_text(item.get("noi_dung"))
            if _is_noisy_text_item(text):
                continue
            key = text.lower()
            if key in seen_kha_nang:
                continue
            seen_kha_nang.add(key)
            clean_kha_nang.append({"noi_dung": text})
        results["KhaNangHocTap"]["items"] = clean_kha_nang

    if isinstance(chuan_tham_khao_items, list) and chuan_tham_khao_items:
        for item in chuan_tham_khao_items:
            if not isinstance(item, dict):
                continue
            noi_dung = _clean_text(item.get("noi_dung"))
            noi_dung_goc = _clean_text(item.get("noi_dung_goc"))
            if not noi_dung_goc and noi_dung:
                item["noi_dung_goc"] = noi_dung

            link = _clean_text(item.get("link"))
            if not link:
                source = _clean_text(item.get("noi_dung_goc")) or noi_dung or ""
                url_matches = re.findall(r"(?:https?://\S+|www\.\S+)", source, flags=re.IGNORECASE)
                if url_matches:
                    normalized_link = url_matches[0].rstrip(".,;)")
                    if normalized_link.lower().startswith("www."):
                        normalized_link = f"https://{normalized_link}"
                    item["link"] = normalized_link


def _build_batches(node_contexts: Dict[str, str]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[str]] = {}
    for node_type, text in node_contexts.items():
        key = _clean_text(text)
        grouped.setdefault(key, []).append(node_type)

    batches: List[Dict[str, Any]] = []
    for text, node_types in grouped.items():
        if not text:
            continue
        batches.append({"text": text, "node_types": sorted(node_types)})
    return batches


async def run_curriculum_etl(
    pdf_path: str,
    output_json: Optional[str] = None,
    temperature: float = 0.1,
    max_text_chars: int = 18000,
) -> Dict[str, Any]:
    if not output_json:
        output_json = _default_output_json_path(pdf_path)

    parser = PDFParser(pdf_path)
    sections = parser.parse_to_sections()

    tables = extract_tables(pdf_path)
    tables_text = extract_tables_text(pdf_path)
    raw_front_text = _extract_raw_pdf_text(pdf_path, max_pages=2)
    program_name = Path(pdf_path).name

    table_fallback_nodes, table_relationships = _extract_table_backed_nodes_and_relationships(
        tables, program_name
    )
    front_fallback_nodes, front_relationships = _extract_front_matter_nodes(raw_front_text)

    node_contexts = _build_node_contexts(sections, tables_text, raw_text=raw_front_text)

    content_text = _clean_text(
        "\n".join(
            [
                node_contexts.get("MucTieuDaoTao", ""),
                node_contexts.get("ViTriViecLam", ""),
                node_contexts.get("KhaNangHocTap", ""),
                node_contexts.get("ChuanDauRa", ""),
                node_contexts.get("ChuanThamKhao", ""),
            ]
        )
    )
    content_fallback_nodes, content_relationships = _extract_content_nodes(content_text)
    context_list_fallback_nodes = _extract_context_list_nodes(node_contexts)

    fallback_nodes = _merge_fallback_maps(
        table_fallback_nodes,
        front_fallback_nodes,
        content_fallback_nodes,
        context_list_fallback_nodes,
    )
    deterministic_relationships = table_relationships + front_relationships + content_relationships

    llm_node_contexts: Dict[str, str] = {}
    for node_type, context in node_contexts.items():
        fallback_payload = fallback_nodes.get(node_type)
        has_fallback = bool(
            fallback_payload
            and isinstance(fallback_payload.get("items"), list)
            and any(_has_meaningful_value(item) for item in fallback_payload.get("items", []))
        )
        if has_fallback:
            continue
        if not _looks_like_relevant_content(node_type, context):
            continue
        llm_node_contexts[node_type] = context

    batches = _build_batches(llm_node_contexts)

    all_prompts = build_etl_node_prompts()
    normalized_results: Dict[str, Dict[str, Any]] = {}

    for node_type, fallback_payload in fallback_nodes.items():
        normalized_results[node_type] = fallback_payload

    for batch in batches:
        node_types = batch["node_types"]
        text = batch["text"][:max_text_chars]
        prompts = {node: all_prompts[node] for node in node_types if node in all_prompts}
        if not prompts:
            continue

        raw = await extract_entities(
            text=text,
            prompt=prompts,
            source=pdf_path,
            temperature=temperature,
            optimize_calls=True,
            fallback_per_prompt=True,
            max_text_chars=max_text_chars,
            llm_retries=3,
            fallback_parallel_limit=2,
            per_prompt_parallel_limit=3,
        )

        raw_results = raw.get("results", {}) if isinstance(raw, dict) else {}
        for node_type in node_types:
            normalized_results[node_type] = _normalize_node_payload(
                node_type, raw_results.get(node_type, {})
            )

    for node_type in NODE_FIELD_SCHEMAS.keys():
        normalized_results.setdefault(node_type, _normalize_node_payload(node_type, {}))

    for node_type, fallback_payload in fallback_nodes.items():
        normalized_results[node_type] = _merge_node_items(
            node_type,
            normalized_results.get(node_type, {"node_type": node_type, "items": []}),
            fallback_payload,
        )

    for node_type, payload in list(normalized_results.items()):
        normalized_results[node_type] = _prune_empty_items(payload)

    _finalize_curriculum_nodes(normalized_results, raw_front_text)

    relationship_text = _clean_text("\n".join([raw_front_text, *node_contexts.values()]))
    relationship_items: List[Dict[str, Any]] = list(deterministic_relationships)
    if relationship_text and _looks_like_relevant_content("ChuanDauRa", relationship_text):
        rel = await extract_relationships(
            text=relationship_text,
            source=pdf_path,
            temperature=temperature,
            max_text_chars=max_text_chars,
            llm_retries=2,
        )
        relationship_items.extend(rel.get("items", []) if isinstance(rel, dict) else [])

    combined_relationships = _dedupe_relationships(relationship_items)

    output: Dict[str, Any] = {
        "source": Path(pdf_path).name,
        "parser_output": {
            "source": sections.get("source"),
            "section_count": len(sections.get("children", [])),
        },
        "table_output": {
            "table_count": len(tables),
            "table_preview": [
                {
                    "page": t.get("page"),
                    "table_index": t.get("table_index"),
                    "text": _clean_text(t.get("text", ""))[:300],
                }
                for t in tables[:5]
            ],
        },
        "batch_plan": [
            {
                "node_types": batch["node_types"],
                "text_chars": len(batch["text"]),
                "text_preview": _clean_text(batch["text"])[:250],
            }
            for batch in batches
        ],
        "results": normalized_results,
        "relationships": combined_relationships,
    }

    save_json_output(output, output_json)
    return output


if __name__ == "__main__":
    import os
    pdf_path = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
    # pdf_path = "data/2024_MT_KHMT.pdf"
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    sample_pdf = os.path.join(base_dir, pdf_path)
    output_json = _default_output_json_path(sample_pdf)

    result = asyncio.run(run_curriculum_etl(sample_pdf, output_json=output_json))
    print(json.dumps({"source": result.get("source"), "saved_to": output_json}, ensure_ascii=False))
