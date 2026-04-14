import re
from pathlib import Path
from typing import Any, Dict, List

import pdfplumber

from app.scripts.term_dictionary import TABLE_RELEVANT_HINTS, normalize_term_variants, normalize_token


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _table_to_text(rows: List[List[Any]]) -> str:
    cleaned_rows: List[str] = []
    for idx, row in enumerate(rows, start=1):
        cells = [_clean_text(cell) for cell in row]
        cells = [cell for cell in cells if cell]
        if cells:
            cleaned_rows.append(f"ROW {idx}: " + " | ".join(cells))
    return "\n".join(cleaned_rows).strip()


def _norm_cell(value: Any) -> str:
    return normalize_token(_clean_text(value))


def _header_signature(row: List[Any]) -> str:
    cells = [_norm_cell(cell) for cell in row]
    cells = [c for c in cells if c]
    return "|".join(cells[:6])


def _looks_like_header(row: List[Any]) -> bool:
    sig = _header_signature(row)
    if not sig:
        return False
    return any(k in sig for k in ["stt", "course", "mã", "ma hp", "học phần", "tín chỉ", "credits"])


def _row_start_number(row: List[Any]) -> int:
    if not row:
        return -1
    head = _clean_text(row[0])
    m = re.match(r"^(\d{1,4})\b", head)
    return int(m.group(1)) if m else -1


def _first_data_row_index(rows: List[List[Any]]) -> int:
    for idx, row in enumerate(rows):
        if _looks_like_header(row):
            continue
        if _row_start_number(row) >= 0:
            return idx
    return -1


def _looks_like_curriculum_table(rows: List[List[Any]], text: str) -> bool:
    if not rows:
        return False

    token_text = normalize_token(text)
    hint_count = sum(1 for hint in TABLE_RELEVANT_HINTS if hint in token_text)
    code_count = len(re.findall(r"\b[A-Z]{1,4}\s*\d{2,4}[A-Z]*\b", text.upper()))

    if hint_count >= 2:
        return True
    if code_count >= 4:
        return True
    if any("khoi kien thuc" in _header_signature(row) for row in rows[:3]):
        return True
    return False


def _similar_col_count(a: List[List[Any]], b: List[List[Any]]) -> bool:
    if not a or not b:
        return False
    a_cols = max((len(r) for r in a if r), default=0)
    b_cols = max((len(r) for r in b if r), default=0)
    return a_cols > 0 and b_cols > 0 and abs(a_cols - b_cols) <= 1


def _is_continuation(prev_table: Dict[str, Any], next_table: Dict[str, Any]) -> bool:
    prev_page = prev_table.get("page")
    next_page = next_table.get("page")
    if not isinstance(prev_page, int) or not isinstance(next_page, int) or next_page != prev_page + 1:
        return False

    prev_rows = prev_table.get("rows", []) or []
    next_rows = next_table.get("rows", []) or []
    if not _similar_col_count(prev_rows, next_rows):
        return False

    prev_header = _header_signature(prev_rows[0]) if prev_rows else ""
    next_header = _header_signature(next_rows[0]) if next_rows else ""
    if prev_header and next_header and prev_header == next_header:
        return True

    prev_data_idx = _first_data_row_index(prev_rows)
    next_data_idx = _first_data_row_index(next_rows)
    if prev_data_idx >= 0 and next_data_idx >= 0:
        prev_n = _row_start_number(prev_rows[prev_data_idx])
        next_n = _row_start_number(next_rows[next_data_idx])
        if prev_n >= 0 and next_n >= 0 and 0 < (next_n - prev_n) < 80:
            return True

    # Most CTDT tables continue on next page with first row as data row.
    if next_rows and not _looks_like_header(next_rows[0]):
        return True

    return False


def _merge_continued_tables(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not tables:
        return []

    merged: List[Dict[str, Any]] = []
    current = dict(tables[0])
    current["rows"] = list(current.get("rows", []) or [])
    current["merged_from_pages"] = [current.get("page")]

    for tb in tables[1:]:
        if _is_continuation(current, tb):
            next_rows = list(tb.get("rows", []) or [])
            if next_rows and current.get("rows") and _looks_like_header(next_rows[0]):
                cur_head = _header_signature(current["rows"][0]) if current["rows"] else ""
                nxt_head = _header_signature(next_rows[0])
                if cur_head and nxt_head and cur_head == nxt_head:
                    next_rows = next_rows[1:]
            current["rows"].extend(next_rows)
            current.setdefault("merged_from_pages", []).append(tb.get("page"))
            current["text"] = _table_to_text(current["rows"])
        else:
            current["text"] = _table_to_text(current.get("rows", []))
            merged.append(current)
            current = dict(tb)
            current["rows"] = list(current.get("rows", []) or [])
            current["merged_from_pages"] = [current.get("page")]

    current["text"] = _table_to_text(current.get("rows", []))
    merged.append(current)
    return merged


def extract_tables(pdf_path: str, merge_multipage: bool = True, relevant_only: bool = True) -> List[Dict[str, Any]]:
    """Extract table blocks from PDF pages via pdfplumber."""
    tables: List[Dict[str, Any]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages):
            found = page.find_tables()
            for idx, table in enumerate(found, start=1):
                rows = table.extract() or []
                text = normalize_term_variants(_table_to_text(rows), preserve_newlines=True)
                if not text:
                    continue

                is_relevant = _looks_like_curriculum_table(rows, text)
                if relevant_only and not is_relevant:
                    continue

                x0, y0, x1, y1 = table.bbox
                tables.append(
                    {
                        "type": "table",
                        "page": page_index + 1,
                        "table_index": idx,
                        "bbox": [x0, y0, x1, y1],
                        "rows": rows,
                        "text": text,
                        "is_relevant": is_relevant,
                    }
                )
    return _merge_continued_tables(tables) if merge_multipage else tables


def extract_tables_text(pdf_path: str) -> str:
    """Flatten all extracted table texts into one string."""
    tables = extract_tables(pdf_path)
    chunks = [table["text"] for table in tables if table.get("text")]
    return "\n\n".join(chunks).strip()


if __name__ == "__main__":
    sample_pdf = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/K51_CTDT_NGANH_AN_TOAN_THONG_TIN.pdf"
    if Path(sample_pdf).exists():
        data = extract_tables(sample_pdf)
        print(data)
        print(f"Extracted {len(data)} tables")
    else:
        print("Sample PDF not found")

