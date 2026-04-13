import json
import os
import re
from statistics import median

import fitz  # PyMuPDF
import pdfplumber


class PDFParser:
    """Parse PDF into heading-based sections with compact plain text content."""

    # Match: "1. ...", "2.1 ...", "3.1.2 ..." (avoid table rows like "10 CT...")
    NUMBERED_HEADING_RE = re.compile(r"^((?:\d+\.)|(?:\d+\.\d+(?:\.\d+)*))(?:\s+)(.+)$")
    NUMBER_ONLY_RE = re.compile(r"^\d+(?:\.\d+)*\.$")
    EMBEDDED_HEADING_RE = re.compile(r"(?<!\S)(\d+(?:\.\d+)*\.?)(?=\s+[A-ZÀ-Ỵ])")
    LIST_ITEM_RE = re.compile(r"^[a-z][\.)]\s+", re.IGNORECASE)

    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.doc = fitz.open(pdf_path)
        self.base_font_size = self._estimate_base_font_size()

    def parse(self):
        """Return a flat stream of blocks: heading or paragraph."""
        blocks = []
        paragraph_lines = []
        prev_y = None
        prev_page = None
        pending_heading_prefix = None

        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num in range(len(self.doc)):
                page = self.doc[page_num]
                table_regions = self._extract_tables(pdf.pages[page_num])
                page_lines = self._extract_lines(page, table_regions)

                events = [
                    {
                        "kind": "line",
                        "y": line["y"],
                        "size": line["size"],
                        "text": line["text"],
                        "line": line,
                    }
                    for line in page_lines
                ]
                events.extend(table_regions)
                events.sort(key=lambda x: x["y"])

                for event in events:
                    if event["kind"] == "table":
                        pending_heading_prefix = None
                        self._flush_paragraph(paragraph_lines, blocks)
                        if event["text"]:
                            blocks.append({"type": "table", "text": event["text"]})
                        prev_y = event["y"]
                        prev_page = page_num
                        continue

                    line = event["line"]
                    fragments = self._split_embedded_headings(event["text"])

                    for fragment in fragments:
                        if not fragment:
                            continue

                        # Some PDFs split heading into 2 lines: "2." then "Mục tiêu ..."
                        if self.NUMBER_ONLY_RE.match(fragment):
                            pending_heading_prefix = fragment
                            continue

                        if pending_heading_prefix:
                            merged = self._clean_text(f"{pending_heading_prefix} {fragment}")
                            is_merged_heading, merged_level = self._detect_heading_text(merged, line)
                            if is_merged_heading:
                                self._flush_paragraph(paragraph_lines, blocks)
                                blocks.append(
                                    {"type": "heading", "text": merged, "level": merged_level}
                                )
                                pending_heading_prefix = None
                                continue
                            pending_heading_prefix = None

                        is_heading, level = self._detect_heading_text(fragment, line)

                        if is_heading:
                            self._flush_paragraph(paragraph_lines, blocks)
                            blocks.append({"type": "heading", "text": fragment, "level": level})
                            continue

                        if prev_y is not None and (
                            page_num != prev_page or abs(event["y"] - prev_y) > event["size"] * 1.8
                        ):
                            self._flush_paragraph(paragraph_lines, blocks)

                        paragraph_lines.append(fragment)

                    prev_y = event["y"]
                    prev_page = page_num

        self._flush_paragraph(paragraph_lines, blocks)
        return blocks

    def build_sections(self, blocks):
        """Build a heading tree and keep each heading content as one compact text field."""
        root = {
            "title": "ROOT",
            "level": 0,
            "source": os.path.basename(self.pdf_path),
            "content": "",
            "children": [],
        }
        stack = [root]

        for block in blocks:
            if block["type"] == "heading":
                level = max(1, block.get("level", 1))
                node = {
                    "title": block["text"],
                    "level": level,
                    "content": "",
                    "children": [],
                }

                while stack and stack[-1]["level"] >= level:
                    stack.pop()

                stack[-1]["children"].append(node)
                stack.append(node)
            else:
                text = block["text"]
                target = stack[-1]
                target["content"] = (
                    text if not target["content"] else f"{target['content']} {text}"
                )

        self._clean_section_text(root)
        return root

    def parse_to_sections(self):
        return self.build_sections(self.parse())

    def _extract_lines(self, page, table_regions):
        records = []
        data = page.get_text("dict")

        for block in data.get("blocks", []):
            for line in block.get("lines", []):
                spans = line.get("spans", [])
                if not spans:
                    continue

                text = self._clean_text(" ".join(span.get("text", "") for span in spans))
                if self._is_noise(text):
                    continue

                avg_size = sum(span.get("size", 0) for span in spans) / max(1, len(spans))
                has_bold = any("bold" in span.get("font", "").lower() for span in spans)
                y = min(span.get("bbox", [0, 0, 0, 0])[1] for span in spans)
                y_bottom = max(span.get("bbox", [0, 0, 0, 0])[3] for span in spans)

                # Skip lines that belong to table regions; table text will come from pdfplumber.
                if self._is_inside_table(y, y_bottom, table_regions):
                    continue

                records.append(
                    {
                        "text": text,
                        "size": avg_size,
                        "bold": has_bold,
                        "y": y,
                        "y_bottom": y_bottom,
                    }
                )

        return sorted(records, key=lambda x: x["y"])

    def _detect_heading_text(self, text, line=None):
        text = self._clean_text(text)

        numbered = self.NUMBERED_HEADING_RE.match(text)
        if numbered:
            numbering = numbered.group(1).rstrip(".")
            level = len([part for part in numbering.split(".") if part])
            return True, level

        # Reject common list/bullet rows to avoid false heading matches.
        if self.LIST_ITEM_RE.match(text):
            return False, 0

        # Reject short code-like references, e.g. (PEO3); (PLO10);
        if re.match(r"^\([A-Za-z]{2,}\d+\);?$", text):
            return False, 0

        word_count = len(text.split())
        is_upper = text.isupper() and 2 <= word_count <= 18
        if is_upper:
            return True, 1

        return False, 0

    def _split_embedded_headings(self, text):
        text = self._clean_text(text)
        if not text:
            return []

        starts = [m.start() for m in self.EMBEDDED_HEADING_RE.finditer(text)]
        if not starts or starts[0] != 0:
            starts = [0] + starts

        parts = []
        for idx, start in enumerate(starts):
            end = starts[idx + 1] if idx + 1 < len(starts) else len(text)
            part = self._clean_text(text[start:end])
            if part:
                parts.append(part)
        return parts

    def _extract_tables(self, plumber_page):
        tables = []
        for table in plumber_page.find_tables():
            raw_rows = table.extract() or []
            text = self._table_to_text(raw_rows)
            if not text:
                continue

            x0, y0, x1, y1 = table.bbox
            tables.append(
                {
                    "kind": "table",
                    "y": y0,
                    "y_bottom": y1,
                    "x0": x0,
                    "x1": x1,
                    "text": text,
                }
            )
        return tables

    def _table_to_text(self, rows):
        cleaned_rows = []
        for row in rows:
            cells = [self._clean_text(str(cell)) if cell is not None else "" for cell in row]
            cells = [cell for cell in cells if cell]
            if cells:
                cleaned_rows.append(" | ".join(cells))
        return self._clean_text(" ; ".join(cleaned_rows))

    def _is_inside_table(self, y_top, y_bottom, table_regions):
        for table in table_regions:
            overlap = min(y_bottom, table["y_bottom"]) - max(y_top, table["y"])
            if overlap > 0:
                return True
        return False

    def _estimate_base_font_size(self):
        sizes = []
        for page_num in range(min(3, len(self.doc))):
            page = self.doc[page_num]
            data = page.get_text("dict")
            for block in data.get("blocks", []):
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        size = span.get("size")
                        if size:
                            sizes.append(size)
        return median(sizes) if sizes else 12

    def _clean_text(self, text):
        return re.sub(r"\s+", " ", text).strip()

    def _is_noise(self, text):
        lowered = text.lower()
        return (
            len(lowered) < 2
            or lowered in {"_", "-"}
            or "http" in lowered
            or "www" in lowered
            or "email" in lowered
        )

    def _flush_paragraph(self, paragraph_lines, blocks):
        if not paragraph_lines:
            return

        text = self._clean_text(" ".join(paragraph_lines))
        if text:
            blocks.append({"type": "paragraph", "text": text})
        paragraph_lines.clear()

    def _clean_section_text(self, node):
        node["content"] = self._clean_text(node.get("content", ""))
        for child in node.get("children", []):
            self._clean_section_text(child)


def save_sections(sections, path="debug_sections.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sections, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    pdf_file = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
    # pdf_file = "data/2024_MT_KHMT.pdf"
    # pdf_file = "data/2019_MT_KTM.pdf"
    parser = PDFParser(pdf_file)
    sections = parser.parse_to_sections()
    save_sections(sections)
    print("Saved parsed sections to debug_sections.json")