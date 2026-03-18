import pdfplumber
import pandas as pd

def extract_curriculum(pdf_path):
    all_rows = []
    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 3,
        "join_tolerance": 3,
    }

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages): 
            tables = page.extract_tables(table_settings=table_settings)
            if not tables:
                continue
                
            for table in tables:
                for row in table:
                    # Làm sạch dữ liệu và xóa xuống dòng 
                    clean_row = [str(cell).replace('\n', ' ').strip() if cell else "" for cell in row]
                    
                    # Header: "T T", "Mã số học phần", "Tên học phần"... 
                    if "Mã số" in clean_row or "Tên học phần" in clean_row:
                        if not all_rows: # Chỉ lấy header một lần duy nhất
                            all_rows.append(clean_row)
                        continue
                    
                    # Nhận diện dòng gộp (Tiêu đề khối kiến thức) 
                    non_empty_cells = [c for c in clean_row if c]
                    if len(set(non_empty_cells)) == 1 and "Khối kiến thức" in clean_row[0]:
                        all_rows.append([f"BLOCK_HEADER:{clean_row[0]}"] + [""] * (len(clean_row)-1))
                    else:
                        all_rows.append(clean_row)

    if all_rows:
        return pd.DataFrame(all_rows[1:], columns=all_rows[0])
    return None