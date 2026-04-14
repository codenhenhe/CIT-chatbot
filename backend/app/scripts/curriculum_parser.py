import re
import json

import pdfplumber

HEADING_PATTERN = re.compile(
    r"""
    ^(?P<heading>
        [IVXLCDM]+\.?                  # I. II. III.
        |
        \d+(?:\.\d+)*(?:\.)?         # 1. 2.1 3.1.2.
        |
        [a-z](?:[\.)])?                 # a. b) c hoặc a
    )\s+(?P<title>[^\n]+)$
    """,
    re.MULTILINE | re.VERBOSE
)

TABLE_CODE_PATTERN = re.compile(r"\b[A-Z]{2,}\s*\d{2,}[A-Z]?\b")


class PDFParser:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path

    def _default_table_settings(self):
        return {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "edge_min_length": 3,
            "min_words_vertical": 2,
            "min_words_horizontal": 1,
            "intersection_tolerance": 3,
        }

    def extract_full_text(self):
        texts = []

        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    texts.append(text)

        full_text = "\n".join(texts)

        return full_text

    def extract_page_elements(self, page, page_number):
        elements = []
        table_settings = self._default_table_settings()
        tables = []

        for table in page.find_tables(table_settings=table_settings):
            rows = table.extract()
            rows = [row for row in rows if any(cell and str(cell).strip() for cell in row)]
            if len(rows) < 2:
                continue

            table_lines = []
            for row in rows:
                cells = [self._clean_cell(cell) for cell in row]
                table_lines.append(" | ".join(cells).strip())

            bbox = table.bbox
            tables.append({
                "type": "table",
                "page": page_number,
                "top": bbox[1],
                "bottom": bbox[3],
                "bbox": bbox,
                "rows": rows,
                "content": "\n".join(table_lines).strip(),
            })

        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        words = [word for word in words if not self._word_in_any_bbox(word, tables)]
        text_lines = self._words_to_lines(words)

        for line in text_lines:
            if not line["text"]:
                continue
            elements.append({
                "type": "text",
                "page": page_number,
                "top": line["top"],
                "bottom": line["bottom"],
                "text": line["text"],
            })

        elements.extend(tables)
        elements.sort(key=lambda item: (item["page"], item["top"], 0 if item["type"] == "text" else 1))
        return elements

    def _clean_cell(self, value):
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    def _normalize_table_block(self, table, page_number):
        raw_rows = table.extract()
        raw_rows = [row for row in raw_rows if any(cell and str(cell).strip() for cell in row)]
        if len(raw_rows) < 2:
            return None

        cleaned_rows = [[self._clean_cell(cell) for cell in row] for row in raw_rows]
        width = max(len(row) for row in cleaned_rows)
        normalized_rows = [row + [""] * (width - len(row)) for row in cleaned_rows]

        header = normalized_rows[0]
        data_rows = normalized_rows[1:]
        has_header = width > 2 and self._looks_like_header_row(header)

        if not has_header:
            header = [f"col_{index + 1}" for index in range(width)]
            data_rows = normalized_rows

        columns = []
        for index, label in enumerate(header):
            column_name = f"col_{index + 1}"
            columns.append({
                "index": index,
                "name": column_name,
                "label": label,
            })

        rows = []
        records = []
        for row_index, row in enumerate(data_rows):
            cells = []
            record = {}
            for col_index, value in enumerate(row):
                cell = {
                    "column": columns[col_index]["name"],
                    "label": columns[col_index]["label"],
                    "value": value,
                }
                cells.append(cell)
                if value:
                    record[columns[col_index]["name"]] = value
            rows.append({"index": row_index, "cells": cells})
            if any(cell["value"] for cell in cells):
                records.append(record)

        return {
            "type": "table",
            "page": page_number,
            "bbox": table.bbox,
            "top": table.bbox[1],
            "bottom": table.bbox[3],
            "raw_rows": raw_rows,
            "header": header,
            "columns": columns,
            "rows": rows,
            "records": records,
            "row_count": len(rows),
            "col_count": len(columns),
            "content": "\n".join(" | ".join(row) for row in normalized_rows).strip(),
        }

    def _looks_like_header_row(self, row):
        joined = " ".join(row).strip()
        if not joined:
            return False
        if re.search(r"\b(TT|MSHP|Mã số|Tên học|Học kỳ|Bắt|Tự|LT|TH|tiên quyết|Ghi chú)\b", joined, re.IGNORECASE):
            return True
        alpha_count = sum(1 for cell in row if re.search(r"[A-Za-zÀ-Ỵà-ỵ]", cell))
        return alpha_count >= max(2, len(row) // 2)

    def _word_in_any_bbox(self, word, tables):
        if not tables:
            return False
        cx = (word["x0"] + word["x1"]) / 2
        cy = (word["top"] + word["bottom"]) / 2
        for table in tables:
            x0, top, x1, bottom = table["bbox"]
            if x0 <= cx <= x1 and top <= cy <= bottom:
                return True
        return False

    def _words_to_lines(self, words, tolerance=3):
        if not words:
            return []

        words = sorted(words, key=lambda word: (round(word["top"] / tolerance) * tolerance, word["x0"]))
        lines = []
        current_words = []
        current_top = None
        current_bottom = None

        def flush():
            nonlocal current_words, current_top, current_bottom
            if not current_words:
                return
            current_words.sort(key=lambda word: word["x0"])
            text = " ".join(word["text"] for word in current_words).strip()
            lines.append({
                "text": re.sub(r"\s+", " ", text).strip(),
                "top": current_top if current_top is not None else 0,
                "bottom": current_bottom if current_bottom is not None else 0,
            })
            current_words = []
            current_top = None
            current_bottom = None

        for word in words:
            word_top = word["top"]
            if current_top is None:
                current_words = [word]
                current_top = word_top
                current_bottom = word["bottom"]
                continue

            if abs(word_top - current_top) <= tolerance:
                current_words.append(word)
                current_bottom = max(current_bottom, word["bottom"])
            else:
                flush()
                current_words = [word]
                current_top = word_top
                current_bottom = word["bottom"]

        flush()
        return lines

    def extract_page_elements(self, page, page_number):
        elements = []
        table_settings = self._default_table_settings()
        tables = []

        for table in page.find_tables(table_settings=table_settings):
            normalized = self._normalize_table_block(table, page_number)
            if not normalized:
                continue

            tables.append(normalized)

        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        words = [word for word in words if not self._word_in_any_bbox(word, tables)]
        text_lines = self._words_to_lines(words)

        for line in text_lines:
            if not line["text"]:
                continue
            elements.append({
                "type": "text",
                "page": page_number,
                "top": line["top"],
                "bottom": line["bottom"],
                "text": line["text"],
            })

        elements.extend(tables)
        elements.sort(key=lambda item: (item["page"], item["top"], 0 if item["type"] == "text" else 1))
        return elements

    def split_by_headings(self, text):
        text = self.normalize_text(text)
        matches = list(HEADING_PATTERN.finditer(text))

        if not matches:
            return []

        sections = []

        for i, match in enumerate(matches):
            start = match.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)

            section_text = self.clean_noise_lines(text[start:end].strip())
            plain_text, table_blocks, blocks = self.split_section_blocks(section_text)

            sections.append({
                "heading": self.normalize_heading(match.group("heading")),
                "title": match.group("title").strip(),
                "raw_content": section_text,
                "content": plain_text,
                "text_content": plain_text,
                "tables": table_blocks,
                "blocks": blocks
            })

        filtered_sections = []
        for sec in sections:
            if self.is_false_heading(sec["heading"], sec["title"]):
                if filtered_sections:
                    prev = filtered_sections[-1]
                    prev["raw_content"] = (prev["raw_content"] + "\n" + sec["raw_content"]).strip()
                    prev["content"] = (prev["content"] + "\n" + sec["content"]).strip()
                    prev["text_content"] = prev["content"]
                    prev["blocks"].extend(sec.get("blocks", []))
                    prev["tables"].extend(sec.get("tables", []))
                continue
            filtered_sections.append(sec)

        return filtered_sections

    def _new_section(self, heading, title):
        return {
            "heading": self.normalize_heading(heading),
            "title": title.strip(),
            "raw_content": "",
            "content": "",
            "text_content": "",
            "tables": [],
            "blocks": [],
        }

    def _append_text_block(self, section, text):
        text = text.strip()
        if not text or section is None:
            return

        block = {"type": "text", "content": text}
        section["blocks"].append(block)
        section["raw_content"] = (section["raw_content"] + "\n" + text).strip() if section["raw_content"] else text
        section["content"] = (section["content"] + "\n" + text).strip() if section["content"] else text
        section["text_content"] = section["content"]

    def _append_table_block(self, section, table_block):
        if section is None:
            return

        section["blocks"].append(table_block)
        section["tables"].append(table_block)
        section["raw_content"] = (section["raw_content"] + "\n" + table_block["content"]).strip() if section["raw_content"] else table_block["content"]

    def _compact_section(self, section):
        text = (section.get("text_content") or section.get("content") or "").strip()
        compact_tables = []
        for table in section.get("tables", []):
            columns = table.get("columns", [])
            labels = []
            for col in columns:
                if isinstance(col, dict):
                    labels.append(col.get("label") or col.get("name") or "")
                else:
                    labels.append(str(col))

            records = table.get("records", [])
            compact_table = {
                "page": table.get("page"),
                "columns": labels,
                "records": records,
                "row_count": table.get("row_count", len(records)),
            }
            if table.get("bbox"):
                compact_table["bbox"] = table.get("bbox")
            compact_tables.append(compact_table)

        return {
            "heading": section.get("heading", ""),
            "title": section.get("title", ""),
            "text_content": text,
            "tables": compact_tables,
        }

    def parse_pdf_by_geometry(self):
        sections = []
        current_section = None
        cover_section = self._new_section("0.", "Trang bìa")

        with pdfplumber.open(self.pdf_path) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):
                elements = self.extract_page_elements(page, page_number)
                for element in elements:
                    if element["type"] == "text":
                        line = self.clean_noise_lines(element["text"])
                        if not line:
                            continue

                        match = HEADING_PATTERN.match(line)
                        if match and not self.is_false_heading(match.group("heading"), match.group("title")):
                            if current_section:
                                sections.append(current_section)
                            current_section = self._new_section(match.group("heading"), match.group("title"))
                            self._append_text_block(current_section, line)
                        else:
                            if current_section is None:
                                self._append_text_block(cover_section, line)
                            else:
                                self._append_text_block(current_section, line)
                        continue

                    table_block = {
                        "page": element["page"],
                        "bbox": element["bbox"],
                        "header": element.get("header", []),
                        "columns": element.get("columns", []),
                        "records": element.get("records", []),
                        "row_count": element.get("row_count", 0),
                        "content": element["content"],
                    }
                    if current_section is None:
                        self._append_table_block(cover_section, table_block)
                    else:
                        self._append_table_block(current_section, table_block)

        if current_section:
            sections.append(current_section)

        has_cover_content = bool((cover_section.get("text_content") or "").strip()) or bool(cover_section.get("tables") or [])
        if has_cover_content:
            sections.insert(0, cover_section)

        compact_sections = [self._compact_section(section) for section in sections]
        return self.build_tree(compact_sections)

    def normalize_text(self, text):
        text = text.replace("\r", "\n")
        text = re.sub(
            r"(?<=\S)\s+(?=(?:\d+\.\d+(?:\.\d+)*|[IVXLCDM]+\.|[a-z][\.)])\s+[A-ZÀ-Ỵ])",
            "\n",
            text,
        )
        return text

    def clean_noise_lines(self, content):
        lines = []
        for line in content.splitlines():
            s = line.strip()
            if not s:
                continue
            # Loại bỏ dòng số trang đơn lẻ: 3, 10, 15...
            if re.fullmatch(r"\d{1,3}", s):
                continue
            lines.append(s)
        return "\n".join(lines)

    def normalize_heading(self, heading):
        heading = heading.strip()
        if re.fullmatch(r"[IVXLCDM]+", heading):
            return heading + "."
        if re.fullmatch(r"\d+(?:\.\d+)*", heading):
            return heading + "."
        if re.fullmatch(r"[a-z]", heading):
            return heading + "."
        return heading

    def is_false_heading(self, heading, title):
        title = title.strip()
        if not title:
            return True
        if title[0] in "-(*[{<":
            return True
        if re.fullmatch(r"[A-Z]{1,4}(?:\s+[A-Z]{1,4})?", title):
            return True
        if TABLE_CODE_PATTERN.match(title):
            return True
        if re.match(r"^[A-Z]{2,}\d", title.replace(" ", "")):
            return True
        if len(re.findall(r"\d+", title)) >= 4:
            return True
        # Dòng bảng thường bắt đầu bằng mã học phần hoặc chuỗi cột
        if re.search(r"\b(TT|MSHP|tín chỉ|tiết LT|tiết TH|học phần tiên quyết|Số Đơn vị|Mã số|Học kỳ|Khối kiến thức|HP)\b", title, re.IGNORECASE):
            return True
        return False

    def split_section_blocks(self, content):
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        if not lines:
            return "", [], []

        blocks = []
        text_lines = []
        table_blocks = []
        current_type = None
        current_lines = []

        def flush():
            nonlocal current_type, current_lines
            if not current_lines:
                return
            block_content = "\n".join(current_lines).strip()
            block = {"type": current_type, "content": block_content}
            if current_type == "table":
                block["lines"] = current_lines[:]
                table_blocks.append(block)
            else:
                text_lines.append(block_content)
            blocks.append(block)
            current_type = None
            current_lines = []

        for line in lines:
            line_type = "table" if self.is_table_line(line) else "text"
            if current_type is None:
                current_type = line_type
                current_lines = [line]
                continue

            if line_type == current_type:
                current_lines.append(line)
                continue

            if current_type == "table" and self.is_table_continuation(line):
                current_lines.append(line)
                continue

            flush()
            current_type = line_type
            current_lines = [line]

        flush()

        plain_text = "\n".join(text_lines).strip()
        return plain_text, table_blocks, blocks

    def is_table_line(self, line):
        if TABLE_CODE_PATTERN.search(line):
            return True
        if len(re.findall(r"\d+", line)) >= 3:
            return True
        if re.fullmatch(r"[A-Z]{1,3}\d{0,2}", line):
            return True
        if re.search(r"\b(Mã số|học phần|tiên quyết|Bắt|Tự|tiết\s*LT|tiết\s*TH|LT\s*TH)\b", line, re.IGNORECASE):
            return True
        if re.search(r"\b(TT|MSHP|Tên học|Chuẩn đ[âa]u ra|Học kỳ)\b", line, re.IGNORECASE):
            return True
        return False

    def is_table_continuation(self, line):
        if not line:
            return False
        if re.fullmatch(r"[A-Z]{1,3}\d{0,2}", line):
            return True
        if re.fullmatch(r"(?:AV|PV|N1|N2|TC|HP|LT|TH|TT|hoặc)", line, re.IGNORECASE):
            return True
        if re.search(r"^[\d≥<>=\-()/*., ]+$", line):
            return True
        if len(line.split()) <= 4 and not re.search(r"[.!?]$", line):
            return True
        return False

    def get_level(self, heading):
        heading = heading.strip()

        if re.fullmatch(r"[IVXLCDM]+\.", heading):
            return 1
        if re.fullmatch(r"\d+(?:\.\d+)*\.", heading):
            return len([p for p in heading.rstrip('.').split('.') if p])
        if re.fullmatch(r"[a-z](?:[\.)])?", heading):
            return 20
        return 99


    def build_tree(self, sections):
        stack = []
        root = []

        for sec in sections:
            level = self.get_level(sec["heading"])
            node = {**sec, "children": []}

            while stack and stack[-1]["level"] >= level:
                stack.pop()

            if stack:
                stack[-1]["node"]["children"].append(node)
            else:
                root.append(node)

            stack.append({"level": level, "node": node})

        return root

    def parse_pdf_by_structure(self):
        return self.parse_pdf_by_geometry()

    def save_json(self, data, path="debug_output.json"):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    # pdf_file = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/K51_CTDT_NGANH_AN_TOAN_THONG_TIN.pdf"
    pdf_file = "data/pdf/ChuyenNganh_DaoTao/pdf/k50/MT_64_7480202_AnToanThongTin.pdf"
    # pdf_file = "data/2024_MT_KHMT.pdf"
    # pdf_file = "data/2019_MT_KTM.pdf"
    parser = PDFParser(pdf_file)
    tree = parser.parse_pdf_by_structure()
    # print(tree)
    parser.save_json(tree)
    print("Saved parsed data to debug_output.json")