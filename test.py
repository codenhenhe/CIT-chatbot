import os
import re
from html import unescape
from pathlib import Path

os.environ['PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK'] = 'True'

from paddlex import create_pipeline
from paddlex.inference.pipelines import load_pipeline_config


OCR_REPLACEMENTS = [
    (r"\bMā só\b", "Mã số"),
    (r"\bMā\b", "Mã"),
    (r"\bhoc phan\b", "học phần"),
    (r"\bHoc phan\b", "Học phần"),
    (r"\bHoc phàn\b", "Học phần"),
    (r"\bphan\b", "phần"),
    (r"\bphàn\b", "phần"),
    (r"\bphān\b", "phân"),
    (r"\bs6\b", "số"),
    (r"\bS6\b", "Số"),
    (r"\btiét\b", "tiết"),
    (r"\btiēt\b", "tiết"),
    (r"\btiēn\b", "tiên"),
    (r"\bhiēn\b", "hiện"),
    (r"\bthurc\b", "thực"),
    (r"\bthue\b", "thực"),
    (r"\bquóc\b", "quốc"),
    (r"\bquoc\b", "quốc"),
    (r"\btien quyet\b", "tiên quyết"),
    (r"\bTienquyet\b", "Tiên quyết"),
    (r"\btiēn quyét\b", "tiên quyết"),
    (r"\bsong hanh\b", "song hành"),
    (r"\bbó trí\b", "bố trí"),
    (r"\bbotri\b", "bố trí"),
    (r"\bbuocchon\b", "bắt buộc/chọn"),
    (r"\bBat T buocchon\b", "bắt buộc/chọn"),
    (r"\bKhói kién thúс\b", "Khối kiến thức"),
    (r"\bGiáo duc\b", "Giáo dục"),
    (r"\bquóc phòng\b", "quốc phòng"),
    (r"\bphân tích\b", "phân tích"),
    (r"\bquan tri\b", "quản trị"),
    (r"\btrí tuē\b", "trí tuệ"),
    (r"\bthong tin\b", "thông tin"),
    (r"\bcong nghe\b", "công nghệ"),
    (r"\bdai hoc\b", "đại học"),
    (r"\btruong\b", "trường"),
    (r"\ban toan thong tin\b", "an toàn thông tin"),
]

COURSE_CODE_RUN_PATTERN = re.compile(r"(?:\b\d+\s+)?[A-Z]{2,}\d{2,}[A-Z]?(?:\b|(?=[,;\s]))")


def _normalize_vietnamese_text(text):
    normalized = text
    for pattern, replacement in OCR_REPLACEMENTS:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _split_course_code_runs(text):
    matches = [match.group(0).strip(" ,;") for match in COURSE_CODE_RUN_PATTERN.finditer(text)]
    if len(matches) > 1:
        return " / ".join(matches)
    return text


def _clean_text(text):
    # Collapse repeated spaces/newlines for cleaner console output.
    text = unescape(text)
    text = _normalize_vietnamese_text(text)
    text = _split_course_code_runs(text)
    return text


def _extract_rows_from_html_table(table_html):
    rows = []
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL):
        cells = []
        for cell_html in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL):
            text = re.sub(r"<[^>]+>", " ", cell_html)
            cells.append(_clean_text(unescape(text)))
        if cells:
            rows.append(cells)
    return rows


def _print_table_preview(table_html, max_rows=8):
    rows = _extract_rows_from_html_table(table_html)
    if not rows:
        print("(Khong doc duoc cau truc hang/cot tu HTML bang)")
        return

    print(f"(Xem truoc {min(len(rows), max_rows)}/{len(rows)} hang)")
    for idx, row in enumerate(rows[:max_rows], 1):
        print(f"  R{idx:02d}: " + " | ".join(row))
    if len(rows) > max_rows:
        print("  ...")


def run_ocr_vietnamese(pdf_path, min_score=0.70):
    # OCR text pipeline.
    ocr_pipeline = create_pipeline(
        pipeline="OCR",
        device="gpu:0"
    )

    ocr_results = ocr_pipeline.predict(input=pdf_path, text_rec_score_thresh=min_score)

    for i, res in enumerate(ocr_results):
        print(f"\n--- Trang {i+1} ---")

        printed = 0

        # PaddleX OCRResult hiện tại là dict-like với rec_texts/rec_scores
        rec_texts = res.get("rec_texts", []) if hasattr(res, "get") else []
        rec_scores = res.get("rec_scores", []) if hasattr(res, "get") else []

        if rec_texts:
            for idx, text in enumerate(rec_texts):
                score = rec_scores[idx] if idx < len(rec_scores) else None
                if score is None:
                    print(text)
                else:
                    print(f"[{score:.2f}] {text}")
                printed += 1

        # Tương thích cấu trúc cũ nếu có page_res/ocr_res
        elif hasattr(res, 'page_res') and res.page_res:
            ocr_res = res.page_res[0].get('ocr_res', [])
            for item in ocr_res:
                print(f"[{item['confidence']:.2f}] {item['text']}")
                printed += 1

        if printed == 0:
            print("(Khong nhan duoc dong van ban nao o trang nay)")

    # Table structure pipeline (override OCR model to OCRv5 for better Vietnamese diacritics).
    table_config = load_pipeline_config("table_recognition")
    table_config["SubPipelines"]["GeneralOCR"]["SubModules"]["TextDetection"]["model_name"] = "PP-OCRv5_server_det"
    table_config["SubPipelines"]["GeneralOCR"]["SubModules"]["TextRecognition"]["model_name"] = "PP-OCRv5_server_rec"
    table_pipeline = create_pipeline(config=table_config, device="gpu:0")

    out_dir = Path("debug_output/tables")
    out_dir.mkdir(parents=True, exist_ok=True)

    total_tables = 0
    for page_idx, table_res in enumerate(table_pipeline.predict(input=pdf_path), 1):
        tables = table_res.get("table_res_list", []) if hasattr(table_res, "get") else []
        if not tables:
            continue

        print(f"\n=== Bang o Trang {page_idx} (so bang: {len(tables)}) ===")
        for t_idx, table in enumerate(tables, 1):
            total_tables += 1
            table_html = table.get("pred_html", "")
            html_path = out_dir / f"page_{page_idx:03d}_table_{t_idx:02d}.html"
            html_path.write_text(table_html, encoding="utf-8")

            cleaned_rows = _extract_rows_from_html_table(table_html)
            cleaned_path = out_dir / f"page_{page_idx:03d}_table_{t_idx:02d}.cleaned.tsv"
            cleaned_path.write_text("\n".join("\t".join(row) for row in cleaned_rows), encoding="utf-8")

            print(f"Bang {t_idx}: da luu HTML -> {html_path}")
            print(f"Bang {t_idx}: da luu TSV  -> {cleaned_path}")
            _print_table_preview(table_html)

    if total_tables == 0:
        print("\n(Khong phat hien bang nao trong tai lieu nay)")
    else:
        print(f"\nTong so bang phat hien: {total_tables}")

if __name__ == "__main__":
    pdf_file = "data/pdf/ChuyenNganh_DaoTao/pdf/k50/64_7480202_AnToanThongTin.pdf"
    run_ocr_vietnamese(pdf_file)