import fitz
import json
import re
import os

class PDFProcessor:

    def __init__(self, pdf_path, output_dir="data/processed"):
        self.pdf_path = pdf_path
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    # Xử lý lỗi Chương
    def normalize_text(self, text):
        return (
            text.replace("IШ", "III")
                .replace("І", "I")
                .replace("Ⅴ", "V")
                .replace("Х", "X")
                .strip()
        )

    def process(self):
        pdf_name = os.path.basename(self.pdf_path)
        out_path = os.path.join(
            self.output_dir,
            os.path.splitext(pdf_name)[0] + ".json"
        )

        doc = fitz.open(self.pdf_path)

        full_text = ""
        for i in range(doc.page_count):
            full_text += "\n" + doc.load_page(i).get_text()

        lines = [self.normalize_text(l) for l in full_text.split("\n") if l.strip()]
        lines = [l for l in lines if not (l.isdigit() and len(l) <= 3)]

        data = {
            "metadata": {"ten_file": pdf_name},
            "quyet_dinh_ban_hanh": {"can_cu": [], "cac_dieu": []},
            "quy_dinh_chi_tiet": []
        }

        current_chapter = None
        current_article = None
        current_khoan = None
        is_regulation_started = False

        i = 0
        while i < len(lines):
            line = lines[i]

            # CHƯƠNG
            if re.match(r"^(Ch[uư]ơng)\s+[IVXLCDM]+", line, re.IGNORECASE):
                is_regulation_started = True
                title = line

                if i + 1 < len(lines) and lines[i+1].isupper() and "ĐIỀU" not in lines[i+1].upper():
                    title += ": " + lines[i+1]
                    i += 1

                current_chapter = {"chuong": line, "ten": title, "cac_dieu": []}
                data["quy_dinh_chi_tiet"].append(current_chapter)
                current_article = current_khoan = None
                i += 1
                continue

            # ĐIỀU
            m_art = re.match(r"^([ĐD][iìíỉĩịeêềếểễệu]{2,4}\s+\d+)\.?\s*(.*)", line, re.IGNORECASE)

            is_real_article = False
            if m_art:
                art_id_raw, art_content_after = m_art.groups()
                if "xem" in line.lower() or art_content_after.strip().startswith(")") or line.strip().startswith("("):
                    is_real_article = False
                else:
                    is_real_article = True

            if m_art and is_real_article:
                raw_art_id = m_art.group(1)
                art_id = re.sub(r"^[ĐD][iìíỉĩịeêềếểễệu]{2,4}", "Điều", raw_art_id, flags=re.IGNORECASE)
                art_title = m_art.group(2)

                j = i + 1
                while j < len(lines) and not re.match(r"^(\d+\.|[a-zđ]\)|[ĐD]|Ch[uư]ơng)", lines[j], re.IGNORECASE):
                    art_title += " " + lines[j]
                    j += 1

                path = f"{current_chapter['chuong']} > {art_id}" if current_chapter else art_id

                current_article = {
                    "id": art_id,
                    "tieu_de": art_title.strip(),
                    "full_path": path,
                    "noi_dung": "",
                    "cac_khoan": []
                }

                if is_regulation_started:
                    current_chapter["cac_dieu"].append(current_article)
                else:
                    data["quyet_dinh_ban_hanh"]["cac_dieu"].append(current_article)

                current_khoan = None
                i = j
                continue

            # KHOẢN
            m_khoan = re.match(r"^(\d+)\.\s+(.*)", line)
            if m_khoan and current_article:
                so, nd = m_khoan.groups()
                current_khoan = {
                    "so": so,
                    "noi_dung": nd,
                    "cac_diem": []
                }
                current_article["cac_khoan"].append(current_khoan)
                i += 1
                continue

            # ĐIỂM
            m_diem = re.match(r"^([a-zđ])\)\s+(.*)", line)
            if m_diem and current_khoan:
                ky, nd = m_diem.groups()
                current_khoan["cac_diem"].append({
                    "ky_hieu": ky,
                    "noi_dung": nd
                })
                i += 1
                continue

            # Nội dung
            elif current_article:
                if current_khoan:
                    if current_khoan["cac_diem"]:
                        current_khoan["cac_diem"][-1]["noi_dung"] += " " + line
                    else:
                        current_khoan["noi_dung"] += " " + line
                else:
                    current_article["noi_dung"] += " " + line

            i += 1

        with open(out_path, "w", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print("Saved:", out_path)
        return out_path