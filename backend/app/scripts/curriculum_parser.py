import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

import fitz
import pandas as pd

try:
    from app.scripts.curriculum_table import extract_partitioned_tables
except ModuleNotFoundError:
    # Support running this file directly: python backend/app/scripts/curriculum_parser.py
    from curriculum_table import extract_partitioned_tables


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


class PDFCurriculumParser:
    def __init__(self, pdf_path, output_dir="processed_data/curriculum/"):
        self.pdf_path = pdf_path
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _extract_fulltext(self) -> str:
        with fitz.open(self.pdf_path) as doc:
            parts: list[str] = []
            for page_idx in range(doc.page_count):
                page = doc.load_page(page_idx)
                text = page.get_text("text")
                if text:
                    parts.append(text.strip())
        return "\n".join(part for part in parts if part)

    def _normalize_heading_key(self, text: str) -> str:
        text = unicodedata.normalize("NFKD", _safe_str(text))
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.lower()
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _is_matrix_heading(self, heading: str) -> bool:
        return "ma tran" in self._normalize_heading_key(heading)

    def _is_curriculum_program_section(self, section: dict[str, Any]) -> bool:
        heading = self._normalize_heading_key(_safe_str(section.get("heading", "")))
        content = self._normalize_heading_key(_safe_str(section.get("content", "")))
        return "khung chuong trinh" in heading or "khung chuong trinh" in content

    def _is_heading(self, line: str) -> bool:
        if not line:
            return False

        line = line.strip()
        if len(line) > 180:
            return False

        if re.match(r"^[-•]\s+", line):
            return False

        if line in {"TT", "LT", "TH", "HP"}:
            return False
        if re.match(r"^[A-Z]{2,}\d{2,}[A-Z]*$", line):
            return False
        if re.match(r"^\(?PLO\d+\)?[;.,:]*$", line, re.IGNORECASE):
            return False
        if re.match(r"^[\W_\d]+$", line):
            return False

        m_num = re.match(r"^(\d+(?:\.\d+)*)([\.)]?)\s+(.+)$", line)
        if m_num:
            marker, sep, title = m_num.groups()
            if "." not in marker and sep == "":
                return False
            max_words = 20 if "." not in marker else 14
            if len(title.split()) <= max_words:
                return True

        if re.match(r"^[IVXLCDM]+\s*[\.)-]\s+.+", line):
            return True
        if re.match(r"^[A-ZĐ]\s*[\.)-]\s+.+", line):
            return True
        if line.isupper() and 2 <= len(line.split()) <= 18 and not re.search(r"\d", line):
            return True

        if re.match(
            r"^(thong tin chung|muc tieu dao tao|chuan dau ra|vi tri viec lam|kha nang hoc tap|khung chuong trinh)",
            line,
            re.IGNORECASE,
        ):
            return True

        return False

    def _remove_inline_noise(self, line: str) -> str:
        cleaned = line
        cleaned = re.sub(r"\bMẫu\s*2\.3\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(
            r"TT\s+Mã\s+số\s+HP\s+Tên\s+học\s+ph[âa]̀?n\s+Số\s+tín\s+chỉ\s+Mô\s+tả\s+tóm\s+tắt\s+học\s+ph[âa]̀?n\s+Đơn\s+vị\s+giảng\s+dạy\s+học\s+ph[âa]̀?n",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        return cleaned.strip()

    def _preprocess_lines(self, fulltext: str) -> list[str]:
        raw_lines = [ln.strip() for ln in fulltext.splitlines() if ln.strip()]
        merged: list[str] = []
        i = 0

        while i < len(raw_lines):
            line = raw_lines[i].strip()

            if re.match(r"^\d{1,3}$", line):
                i += 1
                continue

            line = re.sub(
                r"^\d{1,3}\s+(?=(?:\d+(?:\.\d+)*[\.)]?\s+|[IVXLCDM]+\s*[\.)-]\s+|[a-z]\.[ \t]+))",
                "",
                line,
            )
            line = self._remove_inline_noise(line)

            if re.match(r"^\d+(?:\.\d+)*\.?$", line) and i + 1 < len(raw_lines):
                next_line = raw_lines[i + 1]
                if len(next_line) <= 180:
                    merged.append(f"{line} {next_line}")
                    i += 2
                    continue

            merged.append(line)
            i += 1

        return merged

    def _split_heading_content(self, fulltext: str) -> list[dict[str, Any]]:
        lines = self._preprocess_lines(fulltext)
        sections: list[dict[str, Any]] = []

        current_heading = "MỞ ĐẦU"
        current_content: list[str] = []

        for line in lines:
            if self._is_heading(line):
                if current_content or sections:
                    sections.append(
                        {
                            "heading": current_heading,
                            "content": " ".join(current_content).strip(),
                            "content_type": "text",
                        }
                    )
                current_heading = line
                current_content = []
            else:
                current_content.append(line)

        if current_content or not sections:
            sections.append(
                {
                    "heading": current_heading,
                    "content": " ".join(current_content).strip(),
                    "content_type": "text",
                }
            )

        return sections

    def _group_tables_by_heading(self, tables: list[pd.DataFrame]) -> dict[str, list[pd.DataFrame]]:
        grouped: dict[str, list[pd.DataFrame]] = {}
        for table in tables:
            key = self._normalize_heading_key(_safe_str(table.attrs.get("heading_context", "")))
            grouped.setdefault(key, []).append(table)
        return grouped

    def _apply_tables_to_sections(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Remove matrix sections as requested.
        sections = [s for s in sections if not self._is_matrix_heading(_safe_str(s.get("heading", "")))]

        curriculum_table, general_tables = extract_partitioned_tables(self.pdf_path, merge_multipage=True)
        general_by_heading = self._group_tables_by_heading(general_tables)
        curriculum_assigned = False

        for section in sections:
            heading = _safe_str(section.get("heading", ""))
            key = self._normalize_heading_key(heading)

            if self._is_curriculum_program_section(section) and curriculum_table is not None and not curriculum_assigned:
                section["content"] = curriculum_table
                section["content_type"] = "dataframe"
                curriculum_assigned = True
                continue

            matched = general_by_heading.get(key, [])

            if not matched:
                continue

            if len(matched) == 1:
                section["content"] = matched[0]
                section["content_type"] = "dataframe"
            else:
                section["content"] = matched
                section["content_type"] = "dataframe_list"

        return sections

    def parse(self) -> dict[str, Any]:
        """Return parsed structure in runtime, with DataFrame objects in table sections."""
        pdf_file = Path(self.pdf_path)
        if not pdf_file.exists():
            raise FileNotFoundError(f"Không tìm thấy file PDF: {self.pdf_path}")

        fulltext = self._extract_fulltext()
        sections = self._split_heading_content(fulltext)
        sections = self._apply_tables_to_sections(sections)

        return {
            "metadata": {"ten_file": pdf_file.name},
            "fulltext": fulltext,
            "sections": sections,
        }

    def _serialize_for_json(self, data: dict[str, Any]) -> dict[str, Any]:
        out = {
            "metadata": dict(data.get("metadata", {})),
            "fulltext": _safe_str(data.get("fulltext", "")),
            "sections": [],
        }

        for section in data.get("sections", []):
            head = _safe_str(section.get("heading", ""))
            ctype = _safe_str(section.get("content_type", "text"))
            content = section.get("content", "")

            if isinstance(content, pd.DataFrame):
                payload: Any = {
                    "type": "dataframe",
                    "columns": [str(c) for c in content.columns],
                    "rows": content.fillna("").astype(str).values.tolist(),
                    "source_pages": list(content.attrs.get("source_pages", [])),
                    "heading_context": _safe_str(content.attrs.get("heading_context", "")),
                }
            elif isinstance(content, list) and content and all(isinstance(x, pd.DataFrame) for x in content):
                payload = [
                    {
                        "type": "dataframe",
                        "columns": [str(c) for c in df.columns],
                        "rows": df.fillna("").astype(str).values.tolist(),
                        "source_pages": list(df.attrs.get("source_pages", [])),
                        "heading_context": _safe_str(df.attrs.get("heading_context", "")),
                    }
                    for df in content
                ]
            else:
                payload = _safe_str(content)

            out["sections"].append(
                {
                    "heading": head,
                    "content_type": ctype,
                    "content": payload,
                }
            )

        return out

    def extract(self) -> str:
        data = self.parse()

        pdf_file = Path(self.pdf_path)
        output_path = Path(self.output_dir) / f"{pdf_file.stem}.json"
        json_payload = self._serialize_for_json(data)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(json_payload, f, ensure_ascii=False, indent=2)

        return str(output_path)


if __name__ == "__main__":
    # Example usage
    # sample_pdf = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/K51_CTDT_NGANH_HE_THONG_THONG_TIN_CTCLC.pdf"
    sample_pdf = "data/pdf/ChuyenNganh_DaoTao/pdf/k50/MT_69_7480104C_HeThongThongTin_CTCLC.pdf"
    parser = PDFCurriculumParser(sample_pdf)
    result = parser.parse()  # runtime: section content may be DataFrame
    json_path = parser.extract()  # file output: DataFrame serialized as rows/columns
    print(f"Parsed sections: {len(result.get('sections', []))}")
    print(f"JSON saved to: {json_path}")
