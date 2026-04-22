import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber


class CurriculumTableExtractor:
    """Simple and efficient table extractor for curriculum PDFs."""

    HEADER_HINTS = (
        "tt",
        "ma",
        "học phần",
        "hoc phan",
        "tín chỉ",
        "tin chi",
        "nội dung",
        "noi dung",
    )

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self._cache_merged: list[pd.DataFrame] | None = None
        self._cache_raw: list[pd.DataFrame] | None = None

    @staticmethod
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    @classmethod
    def _normalize_key(cls, value: str) -> str:
        text = unicodedata.normalize("NFKD", cls._clean_text(value))
        text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
        return re.sub(r"\s+", " ", text)

    @classmethod
    def _row_key(cls, row: list[Any]) -> str:
        return "|".join(cls._normalize_key(str(c)) for c in row)

    @staticmethod
    def _looks_like_heading_line(line: str) -> bool:
        line = CurriculumTableExtractor._clean_text(line)
        if not line or len(line) > 160:
            return False
        if re.match(r"^\d+(?:\.\d+)*\.?\s+.+", line):
            return True
        if re.match(r"^[IVXLCDM]+\.\s+.+", line):
            return True
        return line.isupper() and len(line.split()) <= 16

    @classmethod
    def _is_curriculum_heading(cls, heading: str) -> bool:
        return "khung chuong trinh" in cls._normalize_key(heading)

    def _extract_heading_for_table(self, page: pdfplumber.page.Page, table_top: float) -> str:
        try:
            upper = page.crop((0, 0, page.width, max(table_top, 0))).extract_text() or ""
        except Exception:
            upper = page.extract_text() or ""

        lines = [self._clean_text(ln) for ln in upper.splitlines() if self._clean_text(ln)]
        for line in reversed(lines[-50:]):
            if self._looks_like_heading_line(line):
                return line
        return ""

    def _discover_grid(self, rows: list[list[Any]], mode: str) -> list[list[str]]:
        if not rows:
            return []

        cleaned = [[self._clean_text(c) for c in row] for row in rows]
        width = max((len(r) for r in cleaned), default=0)
        rows_2d = [row + [""] * (width - len(row)) for row in cleaned]

        # General tables can safely drop very sparse columns.
        if mode != "general":
            return rows_2d

        n_rows = len(rows_2d)
        n_cols = max((len(r) for r in rows_2d), default=0)
        if n_cols == 0:
            return rows_2d

        keep: list[int] = []
        for c in range(n_cols):
            empty = sum(1 for r in rows_2d if c >= len(r) or not self._clean_text(r[c]))
            if (empty / n_rows) < 0.97:
                keep.append(c)

        if not keep:
            keep = list(range(n_cols))

        return [[r[c] if c < len(r) else "" for c in keep] for r in rows_2d]

    def _is_header_row(self, row: list[str]) -> bool:
        if not row:
            return False

        filled = [self._clean_text(c) for c in row if self._clean_text(c)]
        if len(filled) < 2:
            return False

        first = self._clean_text(row[0]) if row else ""
        if re.match(r"^\d{1,4}\b", first):
            return False

        key = self._normalize_key(" ".join(filled))
        if any(hint in key for hint in self.HEADER_HINTS):
            return True

        short_cells = sum(1 for c in filled if len(c.split()) <= 4)
        return short_cells >= max(2, len(filled) - 1)

    def _build_dataframe(self, rows: list[list[str]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()

        has_header = self._is_header_row(rows[0])
        if has_header:
            header = [c if c else f"col_{i + 1}" for i, c in enumerate(rows[0])]
            data = rows[1:]
        else:
            header = [f"col_{i + 1}" for i in range(len(rows[0]))]
            data = rows

        df = pd.DataFrame(data, columns=header)

        # Drop fully empty rows.
        if not df.empty:
            mask_non_empty = df.apply(lambda r: any(self._clean_text(v) for v in r.tolist()), axis=1)
            df = df[mask_non_empty].reset_index(drop=True)

        # If first row is header, remove duplicated header rows appearing in data.
        if has_header and not df.empty:
            header_key = self._row_key(header)
            keep_mask = [self._row_key(df.iloc[i].tolist()) != header_key for i in range(len(df))]
            df = df[keep_mask].reset_index(drop=True)

        return df

    def _map_cells(self, grid_rows: list[list[str]]) -> pd.DataFrame:
        return self._build_dataframe(grid_rows)

    def _resolve_spans(self, df: pd.DataFrame, mode: str) -> pd.DataFrame:
        if mode == "curriculum_program":
            return self._light_curriculum_cleanup(df)
        return df

    def _semantic_label(self, df: pd.DataFrame, heading: str, mode: str) -> pd.DataFrame:
        df.attrs["heading_context"] = heading
        df.attrs["table_mode"] = mode
        return df

    def _process_table(self, page: pdfplumber.page.Page, page_idx: int, table: Any, mode: str) -> pd.DataFrame | None:
        # 1. Xác định lưới chuẩn (Grid Edges) theo API object của pdfplumber
        # Row/Column mới là object (không subscriptable), nên lấy bbox/cell geometry.
        def _unique_edges(values: list[float], tol: float = 0.8) -> list[float]:
            out: list[float] = []
            for value in sorted(values):
                if not out or abs(value - out[-1]) > tol:
                    out.append(value)
            return out

        row_boxes = [r.bbox for r in getattr(table, "rows", []) if getattr(r, "bbox", None)]
        y_values: list[float] = [b[1] for b in row_boxes] + [b[3] for b in row_boxes]

        # Ưu tiên lấy cạnh cột từ cells để tương thích khi table.cols không tồn tại.
        cells = [c for c in getattr(table, "cells", []) if c]
        x_values: list[float] = []
        if cells:
            x_values = [c[0] for c in cells] + [c[2] for c in cells]

        if not y_values and cells:
            y_values = [c[1] for c in cells] + [c[3] for c in cells]

        y_edges = _unique_edges(y_values)
        x_edges = _unique_edges(x_values)

        if len(y_edges) < 2 or len(x_edges) < 2:
            return None

        num_rows = len(y_edges) - 1
        num_cols = len(x_edges) - 1
        
        # 2. Khởi tạo ma trận rỗng theo kích thước lưới
        grid = [["" for _ in range(num_cols)] for _ in range(num_rows)]

        # 3. Duyệt từng cell thực tế và ánh xạ vào lưới cơ sở
        # table.cells chứa tọa độ (x0, top, x1, bottom) của từng ô (kể cả ô gộp)
        for cell in cells:
            if cell is None: continue
            x0, top, x1, bottom = cell
            
            # Trích xuất text trong vùng tọa độ của ô
            cell_text = page.crop(cell).extract_text()
            content = self._clean_text(cell_text)

            # Tìm xem ô này bao phủ những hàng/cột nào trong lưới chuẩn
            # Dùng sai số nhỏ (tolerance=2) để tránh lệch tọa độ PDF
            r_indices = [i for i, y in enumerate(y_edges[:-1]) if y >= top - 2 and y_edges[i+1] <= bottom + 2]
            c_indices = [j for j, x in enumerate(x_edges[:-1]) if x >= x0 - 2 and x_edges[j+1] <= x1 + 2]

            # ĐIỀN GIÁ TRỊ: Ô gộp bao nhiêu hàng/cột thì điền bấy nhiêu ô trong lưới
            for r in r_indices:
                for c in c_indices:
                    grid[r][c] = content

        # 4. Chuyển ma trận lưới thành DataFrame
        if not grid: return None
        df = self._map_cells(grid)
        
        # Tiếp tục các bước gán nhãn như cũ
        _, y0, _, _ = table.bbox
        heading = self._extract_heading_for_table(page, y0)
        df.attrs["source_pages"] = [page_idx]
        df = self._resolve_spans(df, mode)
        return self._semantic_label(df, heading, mode)

    def _align_columns(self, current: pd.DataFrame, nxt: pd.DataFrame) -> pd.DataFrame:
        if current.empty:
            return nxt

        target = list(current.columns)
        work = nxt.copy()

        if len(work.columns) > len(target):
            work = work.iloc[:, : len(target)]
        elif len(work.columns) < len(target):
            for i in range(len(work.columns), len(target)):
                work[f"__pad_{i}"] = ""

        work.columns = target
        return work

    def _can_merge(self, cur_meta: dict[str, Any], nxt_meta: dict[str, Any]) -> bool:
        if int(nxt_meta["page"]) != int(cur_meta["page"]) + 1:
            return False

        cur_df: pd.DataFrame = cur_meta["df"]
        nxt_df: pd.DataFrame = nxt_meta["df"]
        if cur_df.empty or nxt_df.empty:
            return False

        if abs(len(cur_df.columns) - len(nxt_df.columns)) > 1:
            return False

        cur_head = self._normalize_key(str(cur_meta.get("heading", "")))
        nxt_head = self._normalize_key(str(nxt_meta.get("heading", "")))
        return not (cur_head and nxt_head and cur_head != nxt_head)

    def _drop_repeated_top_rows(self, current: pd.DataFrame, nxt: pd.DataFrame) -> pd.DataFrame:
        if nxt.empty:
            return nxt

        header_key = self._row_key(list(current.columns))
        out = nxt.copy()
        while not out.empty and self._row_key(out.iloc[0].tolist()) == header_key:
            out = out.iloc[1:].reset_index(drop=True)
        return out

    def _merge_split_first_row(self, current: pd.DataFrame, nxt: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if current.empty or nxt.empty:
            return current, nxt

        first = [self._clean_text(v) for v in nxt.iloc[0].tolist()]
        first_col = first[0] if first else ""

        # If first row on next page starts a new numbered row, do not merge.
        if first_col and re.match(r"^\d{1,4}\b", first_col):
            return current, nxt

        if not any(first):
            return current, nxt

        merged = current.copy()
        for col in merged.columns:
            left = self._clean_text(merged.iloc[-1][col])
            right = self._clean_text(nxt.iloc[0][col])
            if right:
                merged.iloc[-1, merged.columns.get_loc(col)] = f"{left} {right}".strip() if left else right

        return merged, nxt.iloc[1:].reset_index(drop=True)

    def _merge_multipage(self, metas: list[dict[str, Any]]) -> list[pd.DataFrame]:
        if not metas:
            return []

        merged: list[pd.DataFrame] = []
        current = metas[0]
        cur_df: pd.DataFrame = current["df"]

        for nxt in metas[1:]:
            nxt_df: pd.DataFrame = nxt["df"]
            if self._can_merge(current, nxt):
                nxt_df = self._align_columns(cur_df, nxt_df)
                nxt_df = self._drop_repeated_top_rows(cur_df, nxt_df)
                cur_df, nxt_df = self._merge_split_first_row(cur_df, nxt_df)
                if not nxt_df.empty:
                    cur_df = pd.concat([cur_df, nxt_df], ignore_index=True)

                pages = list(cur_df.attrs.get("source_pages", []))
                if not pages:
                    pages = [int(current["page"])]
                pages.append(int(nxt["page"]))
                cur_df.attrs["source_pages"] = pages
                cur_df.attrs["heading_context"] = current.get("heading", "") or nxt.get("heading", "")

                current["df"] = cur_df
                current["page"] = int(nxt["page"])
                if not current.get("heading") and nxt.get("heading"):
                    current["heading"] = nxt.get("heading")
            else:
                merged.append(cur_df)
                current = nxt
                cur_df = nxt_df

        merged.append(cur_df)
        return merged

    def _extract_all_tables(self, merge_multipage: bool = True) -> list[pd.DataFrame]:
        cache = self._cache_merged if merge_multipage else self._cache_raw
        if cache is not None:
            return list(cache)

        metas: list[dict[str, Any]] = []
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                for table in page.find_tables():
                    df = self._process_table(page, page_idx, table, mode="general")
                    if df is None or df.empty:
                        continue

                    heading = self._clean_text(df.attrs.get("heading_context", ""))
                    metas.append({"page": page_idx, "heading": heading, "df": df})

        out = self._merge_multipage(metas) if merge_multipage else [m["df"] for m in metas]
        if merge_multipage:
            self._cache_merged = list(out)
        else:
            self._cache_raw = list(out)
        return list(out)

    def _score_curriculum_table(self, df: pd.DataFrame) -> int:
        heading = self._normalize_key(self._clean_text(df.attrs.get("heading_context", "")))
        score = 0
        if self._is_curriculum_heading(heading):
            score += 1000

        header = self._normalize_key(" ".join(str(c) for c in df.columns))
        if "hoc phan" in header:
            score += 120
        if "tin chi" in header:
            score += 120
        if "ma" in header:
            score += 60

        score += len(df) * 2 + len(df.columns)
        return score

    def _is_valid_curriculum_candidate(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False

        heading = self._normalize_key(self._clean_text(df.attrs.get("heading_context", "")))
        header = self._normalize_key(" ".join(str(c) for c in df.columns))
        keyword_hits = sum(1 for token in ("hoc phan", "tin chi", "ma", "hoc phan", "so tin") if token in header)

        if not self._is_curriculum_heading(heading):
            return False

        if len(df.columns) < 4 or len(df) < 4:
            return False

        if keyword_hits >= 2:
            return True

        # Fallback only for curriculum-style tables with strong shape.
        return len(df.columns) >= 8 and len(df) >= 8

    def _light_curriculum_cleanup(self, df: pd.DataFrame) -> pd.DataFrame:
        """Light cleanup for curriculum tables: merge obvious continuation rows without broad fill-down."""
        if df.empty:
            return df

        rows = df.fillna("").astype(str).values.tolist()
        cleaned: list[list[str]] = []

        def non_empty_count(row: list[str]) -> int:
            return sum(1 for cell in row if self._clean_text(cell))

        def is_section_row(row: list[str]) -> bool:
            text = self._normalize_key(" ".join(self._clean_text(c) for c in row))
            return any(
                token in text
                for token in (
                    "khối kiến thức",
                    "khung chương trình",
                    "khung chuong trinh",
                    "cộng:",
                    "cong:",
                    "giáo dục đại cương",
                    "giao duc dai cuong",
                    "chuyên ngành",
                    "chuyen nganh",
                )
            )

        for row in rows:
            first = self._clean_text(row[0]) if row else ""
            has_data = any(self._clean_text(c) for c in row)
            if not has_data:
                continue

            # Merge only obvious wrapped continuation rows.
            if cleaned and not is_section_row(row) and not re.match(r"^\d{1,4}\b", first) and not first and non_empty_count(row) <= 2:
                prev = cleaned[-1]
                for i in range(len(prev)):
                    right = self._clean_text(row[i]) if i < len(row) else ""
                    if right:
                        left = self._clean_text(prev[i]) if i < len(prev) else ""
                        prev[i] = f"{left} {right}".strip() if left else right
                continue

            cleaned.append([self._clean_text(c) for c in row])

        out = pd.DataFrame(cleaned, columns=list(df.columns))
        out.attrs = dict(df.attrs)
        return out

    def extract_partitioned_tables(self, merge_multipage: bool = True) -> tuple[pd.DataFrame | None, list[pd.DataFrame]]:
        """Main entrypoint: return one curriculum-program table and the remaining general tables."""
        tables = self._extract_all_tables(merge_multipage=merge_multipage)
        if not tables:
            return None, []

        candidates: list[tuple[int, int]] = []
        for idx, df in enumerate(tables):
            if self._is_valid_curriculum_candidate(df):
                candidates.append((self._score_curriculum_table(df), idx))

        if not candidates:
            return None, tables

        _, best_idx = max(candidates, key=lambda x: x[0])
        curriculum = self._resolve_spans(tables[best_idx], mode="curriculum_program")
        general = [df for i, df in enumerate(tables) if i != best_idx]
        return curriculum, general

    def export_tables_to_xlsx(self, tables: list[pd.DataFrame], output_path: str) -> str:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            if not tables:
                pd.DataFrame({"message": ["No tables found"]}).to_excel(writer, sheet_name="empty", index=False)
            else:
                for idx, df in enumerate(tables, start=1):
                    df.to_excel(writer, sheet_name=f"table_{idx:02d}", index=False)

        return str(out)


def extract_partitioned_tables(pdf_path: str, merge_multipage: bool = True) -> tuple[pd.DataFrame | None, list[pd.DataFrame]]:
    return CurriculumTableExtractor(pdf_path).extract_partitioned_tables(merge_multipage=merge_multipage)


if __name__ == "__main__":
    # sample_pdf = "data/pdf/ChuyenNganh_DaoTao/pdf/k50/MT_69_7480104C_HeThongThongTin_CTCLC.pdf"
    sample_pdf = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/K51_CTDT_NGANH_HE_THONG_THONG_TIN_CTCLC.pdf"
    if Path(sample_pdf).exists():
        extractor = CurriculumTableExtractor(sample_pdf)
        curriculum_df, general_tables = extractor.extract_partitioned_tables(merge_multipage=True)

        print(f"General tables: {len(general_tables)}")
        print(f"Has curriculum-program table: {curriculum_df is not None}")

        out_dir = Path("processed_data") / "curriculum"
        general_xlsx = out_dir / (Path(sample_pdf).stem + "_general_tables.xlsx")
        print("Saved general XLSX:", extractor.export_tables_to_xlsx(general_tables, str(general_xlsx)))

        if curriculum_df is not None:
            curriculum_xlsx = out_dir / (Path(sample_pdf).stem + "_curriculum_program_table.xlsx")
            print("Saved curriculum XLSX:", extractor.export_tables_to_xlsx([curriculum_df], str(curriculum_xlsx)))
    else:
        print("Sample PDF not found")
