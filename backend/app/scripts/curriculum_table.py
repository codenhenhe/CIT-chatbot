import re
from pathlib import Path
from typing import Any, Dict, List

import pdfplumber


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _table_to_text(rows: List[List[Any]]) -> str:
    cleaned_rows: List[str] = []
    for row in rows:
        cells = [_clean_text(cell) for cell in row]
        cells = [cell for cell in cells if cell]
        if cells:
            cleaned_rows.append(" | ".join(cells))
    return _clean_text(" ; ".join(cleaned_rows))


def extract_tables(pdf_path: str) -> List[Dict[str, Any]]:
    """Extract table blocks from PDF pages via pdfplumber."""
    tables: List[Dict[str, Any]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages):
            found = page.find_tables()
            for idx, table in enumerate(found, start=1):
                rows = table.extract() or []
                text = _table_to_text(rows)
                if not text:
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
                    }
                )
    return tables


def extract_tables_text(pdf_path: str) -> str:
    """Flatten all extracted table texts into one string."""
    tables = extract_tables(pdf_path)
    chunks = [table["text"] for table in tables if table.get("text")]
    return _clean_text("\n".join(chunks))


if __name__ == "__main__":
    sample_pdf = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
    if Path(sample_pdf).exists():
        data = extract_tables(sample_pdf)
        print(f"Extracted {len(data)} tables")
    else:
        print("Sample PDF not found")
