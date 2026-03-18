from ast import pattern
import unicodedata
import pymupdf4llm
import re
import pandas as pd
from app.scripts.table_extractor import extract_curriculum
from llama_index.core.schema import TextNode
from app.scripts.schema import LegalDocProps, MajorProps, DepartmentProps, ProgramProps, FacultyProps, DegreeProps, LevelProps, RefDocProps, TrainingFormProps, TrainingMethodProps, CourseProps, ObjectivesProps, StudyOpportunitiesProps, OutcomeProps
from llama_index.core.node_parser import MarkdownNodeParser
from llama_index.core import Document
from app.scripts.Embedding import EmbeddingModel

class CurriculumETL:
    def __init__(self, pdf_path):
        self.embedder = EmbeddingModel()
        self.pdf_path = pdf_path
        self.nodes = []
        self.edges = []
        self.program_id = "UNKNOWN" 
        self.ma_nganh = "UNKNOWN"
        self.ten_nganh = "UNKNOWN"
        self.loai_hinh_dao_tao = "UNKNOWN"

        self.SECTION_CONCEPTS = {
            "MUC_TIEU": ["mục tiêu", "objectives", "goal"],
            "CHUAN_DAU_RA": ["chuẩn đầu ra", "learning outcomes", "plo", "cdr", "chuẩn đr", "kiến thức kỹ năng thái độ"],
            "VI_TRI_VIEC_LAM": ["vị trí việc làm", "cơ hội nghề nghiệp", "nghề nghiệp", "việc làm", "công việc", "vị trí làm việc"],
            "KHA_NANG_HOC_TAP": ["khả năng học tập", "nâng cao trình độ", "học tập suốt đời", "sau đại học"],
            "KHUNG_CHUONG_TRINH": ["khung chương trình", "danh sách học phần", "kế hoạch đào tạo"],
            "THAM_KHAO": ["tài liệu tham khảo", "tham khảo", "tham chiếu", "căn cứ pháp lý"],
            "THONG_TIN_CHUNG": ["thông tin chung", "tổng quan"]
        }
    
    def _classify_header(self, header_text):
        """Phân loại tiêu đề dựa trên Concept Dictionary"""
        header_lower = header_text.lower()
        
        for concept_type, keywords in self.SECTION_CONCEPTS.items():
            # Check xem tiêu đề có chứa bất kỳ từ khóa nào của concept này không
            if any(kw in header_lower for kw in keywords):
                return concept_type
        
        return "UNKNOWN" # Không nhận diện được

    def _upgrade_markdown_headers(self, md_text):   
        lines = md_text.split('\n')
        new_lines = []
        
        # Regex Cải tiến:
        # 1. (?:[\*_]+)? : Chấp nhận in đậm/nghiêng ở đầu (VD: **1.)
        # 2. (?:Phần|Chương|Mục)? : Chấp nhận từ khóa tiền tố
        # 3. ([IVX]+|\d+(?:\.\d+)*) : GROUP 1 - Bắt Số La Mã hoặc Số Ả Rập (1, 1.2, 3.4.5)
        # 4. (?:[.:)]+)? : Chấp nhận dấu chấm, hai chấm, ngoặc đơn ngay sau số
        # 5. (?:[\*_]+)? : QUAN TRỌNG - Chấp nhận dấu đóng in đậm/nghiêng (VD: **1.**) trước khi có khoảng trắng
        # 6. \s+ : Bắt buộc phải có khoảng trắng
        # 7. (.*) : GROUP 2 - Nội dung
        pattern = re.compile(r'^\s*(?:[\*_]+)?(?:Phần|Chương|Mục)?\s*([IVX]+|\d+(?:\.\d+)*)(?:[.:)]+)?(?:[\*_]+)?\s+(.*)')

        for line in lines:
            stripped_line = line.strip()
            
            # Bỏ qua dòng trống
            if not stripped_line:
                new_lines.append(stripped_line)
                continue

            match = pattern.match(stripped_line)
            
            if match:
                numbering = match.group(1)   # "1" hoặc "2.1" hoặc "I"
                raw_content = match.group(2) # Nội dung thô phía sau
                
                # Xóa các ký tự format Markdown đầu/cuối của nội dung
                # VD: "**Thông tin**" -> "Thông tin"
                clean_content = re.sub(r'^[\*_]+|[\*_]+$', '', raw_content).strip()
                
                # Để phân biệt "Header" và "Câu văn thường"
                is_header = True
                
                # Tiêu chí A: Nếu nội dung rỗng (chỉ có số 1.) -> Không phải header (hoặc header rỗng, bỏ qua)
                if not clean_content:
                    is_header = False
                
                # Tiêu chí B: Độ dài. Header mục lục thường ngắn (< 100 ký tự).
                if len(clean_content) > 100:
                    is_header = False
                
                # Tiêu chí C: Dấu câu kết thúc.
                # Header xịn thường KHÔNG kết thúc bằng dấu chấm (.), chấm phẩy (;), hai chấm (:)
                # Trừ khi nó là câu rất ngắn (VD: "1. Giới thiệu.") thì có thể châm chước.
                # Nhưng nếu dài > 30 ký tự mà có dấu chấm kết thúc -> Khả năng cao là câu văn.
                if len(clean_content) > 30 and clean_content[-1] in ['.', ';', ':']:
                    is_header = False

                if not is_header:
                    new_lines.append(stripped_line)
                    continue
                
                # Số La Mã (I, II) hoặc chữ "Phần" -> Level 2 (##)
                if re.match(r'^[IVX]+$', numbering) or "Phần" in stripped_line:
                    level = 2
                else:
                    # Cấp độ tự động theo số lượng chấm
                    # "1" -> 1 phần -> Level 2 (##)
                    # "1.1" -> 2 phần -> Level 3 (###)
                    # "1.1.1" -> 3 phần -> Level 4 (####)
                    parts = numbering.split('.')
                    level = len(parts) + 1
                
                # Giới hạn max level (Markdown chỉ hỗ trợ đến h6)
                level = min(level, 6)
                hashes = '#' * level
                
                # Tạo dòng Markdown chuẩn
                new_line = f"{hashes} {numbering} {clean_content}"
                new_lines.append(new_line)
                
            else:
                new_lines.append(stripped_line)
                
        return '\n'.join(new_lines)
    
    def _remove_italic_bold(self, text):
        """Xóa định dạng in đậm, in nghiêng trong Markdown"""

        # ***text*** → text
        text = re.sub(r'\*\*\*(.+?)\*\*\*', r'\1', text)

        # Xóa bold (**text** hoặc __text__)
        text = re.sub(r'(\*\*|__)(.+?)\1', r'\2', text)

        # Xóa italic (*text* hoặc _text_)
        text = re.sub(r'(\*|_)(.+?)\1', r'\2', text)

        return text

    def _create_relationship(self, source_id, target_id, rel_type, props=None):
        """Hàm helper để tạo cấu trúc dữ liệu cho cạnh"""
        if not source_id or not target_id: return
        
        edge = {
            "source": source_id,
            "target": target_id,
            "type": rel_type,
            "properties": props if props else {}
        }
        self.edges.append(edge)

    def _normalize_id(self, text):
        """
        Chuẩn hóa chuỗi (ví dụ số quyết định) thành ASCII-only ID
        VD: '3922/QĐ-ĐHCT' -> '3922_QD_DHCT'
        """
        # 1. Chuẩn hóa Unicode + bỏ dấu
        text = unicodedata.normalize('NFD', text)
        text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
        text = text.replace('Đ', 'D').replace('đ', 'd')

        # 2. Thay ký tự không phải chữ/số thành _
        text = re.sub(r'[^A-Za-z0-9]+', '_', text)

        # 3. Xóa _ dư ở đầu/cuối
        return text.strip('_').upper()
    
    # def _generate_id(self, text):
    #     if not text: return "UNKNOWN"
    #      # Bỏ dấu tiếng Việt
    #     text = unicodedata.normalize('NFD', text)
    #     text = text.encode('ascii', 'ignore').decode('utf-8')

    #     # Thay khoảng trắng bằng _
    #     text = re.sub(r'\s+', '_', text)

    #     # Uppercase
    #     return text.upper()


    def contains_markdown_table(text: str) -> bool:
        return bool(re.compile(r'^\s*\|.+\|\s*$', re.MULTILINE).search(text))

    def _process_course_table(self, df):
        """
        Xử lý bảng Khung chương trình đào tạo với cấu trúc phân cấp 3 tầng:
        Khối kiến thức -> Node Yêu cầu (Parent) -> Node Nhóm (Sub) -> Học phần.
        """
        if df is None or df.empty:
            return

        current_block_id = None
        current_parent_group_id = None  # Node yêu cầu lớn (VD: "10 AV hoặc PV")
        current_sub_group_id = None     # Node nhóm con (VD: "AV", "PV")
        
        pat_course_code = re.compile(r'[A-Z]{2,3}\d{3}[A-Z]*')
        pat_tong_tc = re.compile(r'Cộng.*?(\d+)\s*TC', re.IGNORECASE)
        pat_bb_tc = re.compile(r'Bắt buộc.*?(\d+)\s*TC', re.IGNORECASE)
        pat_tu_chon_tc = re.compile(r'Tự chọn.*?(\d+)\s*TC', re.IGNORECASE)
        
        created_virtual_nodes = set()

        for _, row in df.iterrows():
            # Lấy dữ liệu theo vị trí cột (STT:0, Mã:1, Tên:2, TC:3, BB:4, TC:5, TQ:8, SH:9)
            col_marker = str(row.iloc[0]).strip()
            col_ma_hp = str(row.iloc[1]).strip().replace(' ', '')
            col_ten_hp = str(row.iloc[2]).strip()
            col_tc = str(row.iloc[3]).strip()
            col_bat_buoc = str(row.iloc[4]).strip()
            col_tu_chon = str(row.iloc[5]).strip()
            col_so_tiet_lt = str(row.iloc[6]).strip()
            col_so_tiet_th = str(row.iloc[7]).strip()
            col_tien_quyet = str(row.iloc[8]).strip()
            col_song_hanh = str(row.iloc[9]).strip()

            # --- 1. XỬ LÝ BLOCK HEADER VÀ RESET TRẠNG THÁI ---
            if "BLOCK_HEADER:" in col_marker:
                current_parent_group_id = None
                current_sub_group_id = None
                ten_khoi = col_marker.replace("BLOCK_HEADER:", "").strip()
                current_block_id = self._normalize_id(f"{self.program_id}_{ten_khoi}")
                
                if not any(n.id_ == current_block_id for n in self.nodes):
                    block_node = TextNode(
                        id_=current_block_id,
                        metadata={
                            "type": "KhoiKienThuc",
                            "ten_khoi": ten_khoi,
                            "tong_tin_chi": 0, "tin_chi_bat_buoc": 0, "tin_chi_tu_chon": 0
                        }
                    )
                    self.nodes.append(block_node)
                    self._create_relationship(self.program_id, current_block_id, "CO_KHOI_KIEN_THUC")
                continue

            # --- 2. XỬ LÝ DÒNG TỔNG KẾT (CỘNG) ---
            if col_marker.startswith("Cộng"):
                current_parent_group_id = None
                current_sub_group_id = None
                if current_block_id:
                    row_text = " ".join([str(c) for c in row if c])
                    match_tong = pat_tong_tc.search(row_text)
                    match_bb = pat_bb_tc.search(row_text)
                    match_tc = pat_tu_chon_tc.search(row_text)
                    for n in self.nodes:
                        if n.id_ == current_block_id:
                            n.metadata.update({
                                "tong_tin_chi": int(match_tong.group(1)) if match_tong else 0,
                                "tin_chi_bat_buoc": int(match_bb.group(1)) if match_bb else 0,
                                "tin_chi_tu_chon": int(match_tc.group(1)) if match_tc else 0
                            })
                            search_text = (
                                f"{n.metadata['ten_khoi']} - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}) có "
                                f"số tín chỉ bắt buộc: {n.metadata['tin_chi_bat_buoc']}, "
                                f"số tín chỉ tự chọn: {n.metadata['tin_chi_tu_chon']}, "
                                f"tổng cộng: {n.metadata['tong_tin_chi']} tín chỉ."
                            )
                            n.text = search_text
                            break
                continue

            # --- 3. LOGIC PHÂN CẤP NHÓM TỰ CHỌN
            # Reset nếu gặp môn bắt buộc (số đơn ở cột Bắt buộc)
            if col_bat_buoc.isdigit():
                current_parent_group_id = None
                current_sub_group_id = None
            
            # A. Nhận diện Node Cha (Yêu cầu tổng - VD: "10 AV hoặc PV")
            if col_tu_chon and col_tu_chon.strip():
                req_text = col_tu_chon.strip()
                current_parent_group_id = self._normalize_id(f"{current_block_id}_REQ_{req_text}")
                
                if current_parent_group_id not in created_virtual_nodes:
                    p_node = TextNode(
                        id_=current_parent_group_id,
                        text=f"Số tín chỉ yêu cầu tự chọn trong khối {current_block_id} - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {req_text}",
                        metadata={
                            "type": "YeuCauTuChon",
                            "noi_dung_yeu_cau": req_text,
                            "mo_ta": f"Yêu cầu tự chọn trong khối {current_block_id}"
                        }
                    )
                    self.nodes.append(p_node)
                    created_virtual_nodes.add(current_parent_group_id)
                    self._create_relationship(current_block_id, current_parent_group_id, "CO_YEU_CAU_TU_CHON")
            
            # B. Nhận diện Node Con (Nhánh thành phần - VD: "AV" hoặc "PV")
            if col_bat_buoc and not col_bat_buoc.isdigit() and current_parent_group_id:
                sub_label = col_bat_buoc.strip()
                current_sub_group_id = self._normalize_id(f"{current_parent_group_id}_SUB_{sub_label}")
                
                if current_sub_group_id not in created_virtual_nodes:
                    s_node = TextNode(
                        id_=current_sub_group_id,
                        text=f"Nhóm học phần tự chọn - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {sub_label}",
                        metadata={
                            "type": "NhomHocPhanTuChon",
                            "ten_nhom": sub_label
                        }
                    )
                    self.nodes.append(s_node)
                    created_virtual_nodes.add(current_sub_group_id)
                    self._create_relationship(current_parent_group_id, current_sub_group_id, "CO_NHOM_THANH_PHAN")

            # --- 4. TẠO NODE HỌC PHẦN VÀ LIÊN KẾT THEO PHÂN CẤP ---
            ma_hps = pat_course_code.findall(col_ma_hp)
            if not ma_hps:
                continue

            for hp_code in ma_hps:
                hp_id = self._normalize_id(hp_code)
                clean_ten_hp = col_ten_hp.replace("(*)", "").strip()

                search_text = (
                    f"Học phần {hp_code} - {clean_ten_hp} có số tín chỉ là {col_tc}, "
                    f"số tiết lý thuyết là {col_so_tiet_lt if col_so_tiet_lt else '0'}, "
                    f"số tiết thực hành là {col_so_tiet_th if col_so_tiet_th else '0'}."
                )
                
                course_node = TextNode(
                    text=search_text,
                    id_=hp_id,
                    metadata={
                        "type": "HocPhan",
                        "ma_hp": hp_code,
                        "ten_hp": clean_ten_hp,
                        "so_tin_chi": col_tc,
                        "so_tiet_ly_thuyet": col_so_tiet_lt if col_so_tiet_lt else 0,
                        "so_tiet_thuc_hanh": col_so_tiet_th if col_so_tiet_th else 0,
                        "bat_buoc": True if not current_parent_group_id else False,
                    }
                )
                self.nodes.append(course_node)
                
                if current_block_id:
                    # Ưu tiên liên kết vào nhóm con thấp nhất hiện có
                    if current_sub_group_id:
                        self._create_relationship(current_sub_group_id, hp_id, "GOM_HOC_PHAN")
                    elif current_parent_group_id:
                        self._create_relationship(current_parent_group_id, hp_id, "GOM_HOC_PHAN")
                    else:
                        self._create_relationship(current_block_id, hp_id, "GOM_HOC_PHAN")

                # Thiết lập Quan hệ Tiên quyết/Song hành
                for pr in pat_course_code.findall(col_tien_quyet):
                    self._create_relationship(hp_id, self._normalize_id(pr), "YEU_CAU_TIEN_QUYET")
                for cr in pat_course_code.findall(col_song_hanh):
                    self._create_relationship(hp_id, self._normalize_id(cr), "CO_THE_SONG_HANH")

    def _group_nodes(self, nodes):
        """
        Gom nhóm các node con (###, ####) vào node cha (##) gần nhất.
        """
        grouped_nodes = []
        current_section_node = None
        
        for node in nodes:

            content = node.text.strip()
            
            # Kiểm tra xem đây có phải là Header cấp 2 (##) không?
            # Regex: Bắt đầu bằng ## và theo sau là số (VD: ## 1., ## 2.1)
            is_major_header = re.match(r'^##\s+\d+', content)
            
            if is_major_header:
                # 1. Nếu đang có một section cũ đang mở -> Lưu lại xong xuôi
                if current_section_node:
                    grouped_nodes.append(current_section_node)
                
                # 2. Bắt đầu một Section mới (Clone node hiện tại làm gốc)
                # Ta tạo copy để tránh tham chiếu lằng nhằng
                current_section_node = TextNode(
                    text=node.text,
                    metadata=node.metadata.copy()
                )
                
            else:
                if current_section_node:
                    # GỘP NỘI DUNG: Thêm xuống dòng và nối vào node cha
                    current_section_node.text += "\n\n" + node.text
                else:
                    # Trường hợp đặc biệt: Text nằm đầu file trước khi có Header ## đầu tiên
                    # (Ví dụ: Thông tin trường, Quyết định ban hành...)
                    # Ta cứ thêm vào list như một node độc lập (Metadata Node)
                    grouped_nodes.append(node)
        
        # Đừng quên lưu nốt section cuối cùng sau khi hết vòng lặp
        if current_section_node:
            grouped_nodes.append(current_section_node)
            
        return grouped_nodes
    
    def _parse_quyet_dinh_ban_hanh(self, text, full_text):
        """Phân tích trích xuất Văn bản pháp lý và Trình độ"""

        text = re.sub(r'^#+\s+', '', text).strip()  

        # Loại
        loai_match = re.search(r"(Quyết định|Thông tư|Nghị quyết|Luật|Nghị định|Chỉ thị|Thông báo)", text, re.I)
        so_match = re.search(r"(?:số|Số)\s*[:.\-]?\s*([\w\d\-/.]+)", text)
        agency_pattern = r'của\s+(.+?)\s+(về|ban hành|phê duyệt|quy định|thông qua|ban hành)'
        agency_match = re.search(agency_pattern, text, re.I)

        co_quan_ban_hanh = ""
        ten = ""

        if agency_match:
            co_quan_ban_hanh = agency_match.group(1).strip()
            # start_index = agency_match.end()
            content_part = text[agency_match.start(2):].strip()
            ten = content_part[0].upper() + content_part[1:]
        else:
            agency_fallback = re.search(r'của\s+([^)_]+)', text, re.I)
            if agency_fallback:
                co_quan_ban_hanh = agency_fallback.group(1).strip()

        ngay_word_match = re.search(r"ngày\s+(\d+)\s+tháng\s+(\d+)\s+năm\s+(\d+)", full_text, re.I)
        if ngay_word_match:
            d, m, y = ngay_word_match.group(1), ngay_word_match.group(2), ngay_word_match.group(3)
        else:
            ngay_num_match = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", text)
            d, m, y = ngay_num_match.group(1), ngay_num_match.group(2), ngay_num_match.group(3)

        vbpl_id = self._normalize_id(so_match.group(1).strip()) if so_match else "UNKNOWN"
        node = TextNode(
            text=f"Chương trình đào tạo ngành {self.ten_nganh} được ban hành theo {text.strip()}", 
            id_=vbpl_id,
            metadata={
                "type": "VanBanPhapLy",
                LegalDocProps.SO_HIEU: so_match.group(1).strip() if so_match else "",
                LegalDocProps.TEN_VB: ten,
                LegalDocProps.LOAI_VB: loai_match.group(1).strip() if loai_match else "",
                LegalDocProps.NGAY_BAN_HANH: f"{d.zfill(2)}/{m.zfill(2)}/{y}",
                LegalDocProps.CO_QUAN: co_quan_ban_hanh,
                LegalDocProps.NOI_DUNG_GOC: text.strip()
            }
        )
        self.nodes.append(node)   

        self._create_relationship(
            source_id=self.program_id,
            target_id=vbpl_id,
            rel_type="DUOC_BAN_HANH_THEO"
        )

        # Node Trình độ
        pattern = r'TRÌNH\s+ĐỘ\s+([A-ZÀ-Ỹ\s]+?)(?=\s*\(|\s*$)'
        m = re.search(pattern, text, re.IGNORECASE)

        trinh_do = m.group(1).strip() if m else None
        id_trinh_do = self._normalize_id(trinh_do)

        level_node = TextNode(
            text=f"Trình độ đào tạo - Ngành {self.ten_nganh}: {trinh_do}", 
            id_=id_trinh_do,
            metadata={
                "type": "TrinhDo",
                LevelProps.TEN: trinh_do,
            }
        )

        self.nodes.append(level_node)  

        self._create_relationship(
            source_id=self.program_id,
            target_id=id_trinh_do,
            rel_type="DAO_TAO_TRINH_DO"
        )
        
    def _parse_major(self, content):
        """Trích xuất ngành"""

        cleaned_content = self._remove_italic_bold(content)
        # Mã ngành
        ma_nganh_match = re.search(r'Mã ngành:\s*(\d+)', cleaned_content, re.IGNORECASE)
        ma_nganh = ma_nganh_match.group(1).strip() if ma_nganh_match else "Unknown"
        self.ma_nganh = ma_nganh

        # Ngành
        ten_nganh_match = re.search(r'(Ngành|Ngành học):\s*([^\(]+?\s*\([^\)]+\))', cleaned_content, re.IGNORECASE)
        ten_nganh = ten_nganh_match.group(2).strip() if ten_nganh_match else "Unknown"
        vi, en = re.match(r'(.+?)\s*\((.+)\)', ten_nganh).groups()
        ten_nganh_vi = vi.strip()
        ten_nganh_en = en.strip()
        self.ten_nganh = f"{ten_nganh_vi} ({ten_nganh_en})"

        node = TextNode(
            text=f"{ten_nganh_vi} ({ten_nganh_en})",
            id_=ma_nganh,
            metadata={
                "type": "Nganh",
                MajorProps.TEN_VI: ten_nganh_vi,
                MajorProps.TEN_EN: ten_nganh_en,
            }
        )
        self.nodes.append(node)  

    def _parse_unit(self, content):
        """Trích xuất Đơn vị quản lý"""

        unit_match = re.search(r'Đơn vị quản lý:\s*(.+)', content, re.IGNORECASE)
        ten_don_vi = unit_match.group(1).strip() if unit_match else "Unknown"

        bo_mon = re.search(r'Bộ môn\s+([^-\n]+)', ten_don_vi, re.IGNORECASE)
        if bo_mon:
            subunit = bo_mon.group(1).strip()
            ma_don_vi = self._normalize_id(subunit)
            node = TextNode(
                text=f"Ngành {self.ten_nganh} thuộc Bộ môn: {subunit}",
                id_=ma_don_vi,
                metadata={
                    "type": "BoMon",
                    DepartmentProps.TEN: subunit
                }
            )
          
        else:
            khoa = re.search(r'Khoa\s+([^,\n-]+)', ten_don_vi, re.IGNORECASE)
            subunit = khoa.group(1).strip()
            ma_don_vi = self._normalize_id(subunit)
            node = TextNode(
                text=f"Ngành {self.ten_nganh} thuộc Khoa: {subunit}",
                id_=ma_don_vi,
                metadata={
                    "type": "Khoa",
                    FacultyProps.TEN: subunit
                }
            )

        self.nodes.append(node)  

        self._create_relationship(
            source_id=self.ma_nganh,
            target_id=ma_don_vi,
            rel_type="THUOC_VE"
        ) 

    def _parse_curriculum(self, content, full_text):
        """Trích xuất chương trình đào tạo"""

        # Khóa
        nam_match = re.search(r'năm\s+(\d{4})', full_text, re.IGNORECASE)
        nam = nam_match.group(1) if nam_match else None
        k = int(nam) - 1974

        if "chất lượng cao" in full_text.lower():
            loai_hinh = "Chất lượng cao"
            id = f"{self.ma_nganh}_{k}_CLC"
            ngon_ngu = "Tiếng Anh"
        else:
            loai_hinh = "Đại trà"
            id = f"{self.ma_nganh}_{k}_STD"
            ngon_ngu = "Tiếng Việt"

        self.program_id = id

        # Số tín chỉ
        so_tin_chi_match = re.search(r'(Số lượng tín chỉ|Tổng cộng)[:-]\s*(\d+)', content, re.IGNORECASE)
        so_tin_chi = so_tin_chi_match.group(2).strip() if so_tin_chi_match else "Unknown"

        # Thời gian đào tạo
        thoi_gian_dao_tao_match = re.search(r'Thời gian đào tạo:\s*([\d.,]+)', content, re.IGNORECASE)
        thoi_gian_dao_tao = thoi_gian_dao_tao_match.group(1).strip() if thoi_gian_dao_tao_match else "Unknown"
        thoi_gian_dao_tao = thoi_gian_dao_tao.replace(",", ".")

        # Loại văn bằng
        loai_van_bang_match = re.search(r'(Loại văn bằng|Danh hiệu):\s*(.+)', content, re.IGNORECASE)
        loai_van_bang = loai_van_bang_match.group(2).strip() if loai_van_bang_match else "Unknown"

        # Hình thức đào tạo
        hinh_thuc_dao_tao_match = re.search(r'Hình thức đào tạo:\s*(.+)', content, re.IGNORECASE)
        hinh_thuc_dao_tao = hinh_thuc_dao_tao_match.group(1).strip() if hinh_thuc_dao_tao_match else "Unknown"

        # Phương thức tổ chức
        phuong_thuc_to_chuc_match = re.search(r'Phương thức tổ chức đào tạo:\s*(.+)', content, re.IGNORECASE)
        phuong_thuc_to_chuc = phuong_thuc_to_chuc_match.group(1).strip() if phuong_thuc_to_chuc_match else "Unknown"

        search_text = (
            f"Chương trình đào tạo ngành {self.ten_nganh} khóa {k}, loại hình {loai_hinh}. "
            f"Tổng số tín chỉ yêu cầu: {so_tin_chi}. Thời gian đào tạo: {thoi_gian_dao_tao} năm. "
            f"Ngôn ngữ giảng dạy: {ngon_ngu}."
        )

        # Node Chương trình đào tạo
        program_node = TextNode(
            text=search_text,
            id_=id,
            metadata={
                "type": "ChuongTrinhDaoTao",
                ProgramProps.MA_CHUONG_TRINH: id,
                ProgramProps.KHOA_HOC: k,
                ProgramProps.LOAI_HINH: loai_hinh,
                ProgramProps.NGON_NGU: ngon_ngu,
                ProgramProps.TONG_TIN_CHI: int(so_tin_chi),
                ProgramProps.THOI_GIAN: float(thoi_gian_dao_tao),
            }
        )

        self.loai_hinh_dao_tao = loai_hinh

        self.nodes.append(program_node)  

        self._create_relationship(
            source_id=id,
            target_id=self.ma_nganh,
            rel_type="THUOC_VE"
        )

        # Node Loại văn bằng 
        if "cử nhân" in loai_van_bang.lower():
            degree_id = "BACHELOR"
        elif "kỹ sư" in loai_van_bang.lower():
            degree_id = "ENGINEER"
        elif "thạc sĩ" in loai_van_bang.lower():
            degree_id = "MASTER"
        elif "tiến sĩ" in loai_van_bang.lower():
            degree_id = "DOCTOR"
        else:
            degree_id = "OTHER"

        degree_node = TextNode(
            text=f"Loại văn bằng - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {loai_van_bang}",
            id_=degree_id,
            metadata={
                "type": "LoaiVanBang",
                DegreeProps.TEN: loai_van_bang,
            }
        )

        self.nodes.append(degree_node)  

        self._create_relationship(
            source_id=self.program_id,
            target_id=degree_id,
            rel_type="CO_LOAI_VAN_BANG"
        )

        # Node Hình thức đào tạo
        if hinh_thuc_dao_tao != "Unknown":
            forms = re.split(r',|\bvà\b', hinh_thuc_dao_tao)
            for form in forms:
                form = form.strip()
                if not form:
                    continue

                form_id = self._normalize_id(form)
                form_node = TextNode(
                    text=f"Hình thức đào tạo - Ngành {self.ten_nganh}: {form}",
                    id_=form_id,
                    metadata={
                        "type": "HinhThucDaoTao",
                        TrainingFormProps.TEN: form,
                    }
                )
                self.nodes.append(form_node)

                self._create_relationship(
                    source_id=self.program_id,
                    target_id=form_id,
                    rel_type="DAO_TAO_THEO_HINH_THUC"
                )

        # Phương thức tổ chức đào tạo
        if phuong_thuc_to_chuc != "Unknown":
            methods = re.split(r',|\bvà\b', phuong_thuc_to_chuc)
            for method in methods:
                method = method.strip()
                if not method:
                    continue
                if "trực tuyến" in method.lower():
                    method_id = "DAO_TAO_TRUC_TUYEN"
                elif "trực tiếp" in method.lower():
                    method_id = "DAO_TAO_TRUC_TIEP"
                else:
                    method_id = self._normalize_id(method)
                method_node = TextNode(
                    text=f"Phương thức đào tạo - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {method}",
                    id_=method_id,
                    metadata={
                        "type": "PhuongThucDaoTao",
                        TrainingMethodProps.TEN: method,
                    }
                )
                self.nodes.append(method_node)

                self._create_relationship(
                    source_id=self.program_id,
                    target_id=method_id,
                    rel_type="DAO_TAO_THEO_PHUONG_THUC"
                )

                self._create_relationship(
                    source_id=method_id,
                    target_id="quydinhdaotaotructuyen" if method_id == "DAO_TAO_TRUC_TUYEN" else "quychehocvu",
                    rel_type="CO_QUY_DINH"
                )


    def _parse_general_information(self, content, full_text):
        """Trích xuất node ngành, Khoa/bộ môn (đơn vị quản lý), CTĐT trong mục Thông tin chung"""
        self._parse_major(content)
        self._parse_unit(content)
        self._parse_curriculum(content, full_text)
    
    def _parse_program_objectives(self, text):
        """Trích xuất mục tiêu đào tạo"""

        parts = re.split(r'(^###\s+.*)', text, flags=re.MULTILINE)
        parts = [x for x in parts if x != ""]
        # for part in parts:
        #     print(f"Part:\n{part}\n{'-'*20}")

        if len(parts) < 2:
            id = f"{self.program_id}_chung"
            content = text.strip().replace("\n", " ")

            obj_node = TextNode(
                text=f"Mục tiêu đào tạo - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {content}",
                id_=id,
                metadata={
                    "type": "MucTieuDaoTao",
                    ObjectivesProps.LOAI: "Chung",
                    ObjectivesProps.NOI_DUNG: content,
                }
            )
            self.nodes.append(obj_node)

            self._create_relationship(
                source_id=self.program_id,
                target_id=id,
                rel_type="CO_MUC_TIEU_DAO_TAO"
            )
        else:
            # Duyệt qua từng phần đã tách
            for i in range(0, len(parts), 2):
                header = parts[i].lower()       
                section_content = parts[i+1]    

                if "chung" in header.lower():
                    # Xử lý Mục tiêu chung
                    id = f"{self.program_id}_chung"
                    clean_text = section_content.strip().replace("\n", " ")
                    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                    obj_node = TextNode(
                        text=f"Mục tiêu đào tạo chung - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {clean_text}",
                        id_=id,
                        metadata={
                            "type": "MucTieuDaoTao",
                            ObjectivesProps.LOAI: "Chung",
                            ObjectivesProps.NOI_DUNG: clean_text,
                        }
                    )
                    self.nodes.append(obj_node)

                    self._create_relationship(
                        source_id=self.program_id,
                        target_id=id,
                        rel_type="CO_MUC_TIEU_DAO_TAO"
                    )
                        
                else:
                    # Xử lý Mục tiêu cụ thể (PEOs)
                    normalized_content = section_content.replace('\n', ' ')
                    normalized_content = re.sub(r'\s+', ' ', normalized_content)    

                    pattern = r'(?:\s|^)(?P<index>[a-z0-9]{1,2})[.)]?\s+(?P<title>.*?)\((?P<id>PEO\s*\d+)\)'

                    for idx, m in enumerate(re.finditer(pattern, normalized_content)):

                        subid = m.group("id").replace(" ", "")
                        id = f"{self.program_id}_{subid}"

                        raw_title = m.group("title").strip()
                        clean_title = re.sub(r'[,;.]+$', '', raw_title).strip()

                        obj_node = TextNode(
                            text=f"Mục tiêu đào tạo cụ thể - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {clean_title}",
                            id_=id,
                            metadata={
                                "type": "MucTieuDaoTao",
                                ObjectivesProps.LOAI: "CuThe",
                                ObjectivesProps.NOI_DUNG: clean_title,
                            }
                        )
                        self.nodes.append(obj_node)

                        self._create_relationship(
                            source_id=self.program_id,
                            target_id=id,
                            rel_type="CO_MUC_TIEU_DAO_TAO"
                        )

    def _parse_job_positions(self, text):
        """Tách các dòng gạch đầu dòng (-) thành Node ViTriViecLam"""
        # Tách theo dấu gạch đầu dòng hoặc xuống dòng
        jobs = re.split(r'\n-|\n\+', text)
        for idx, job in enumerate(jobs):
            normalized_job = re.sub(r'^[+-]\s*', '', job, count=1).strip().replace("\n", " ")
            final_job = re.sub(r'\s+', ' ', normalized_job)

            job_id = f"{self.program_id}_vtvl_{idx+1}"

            obj_node = TextNode(
                text=f"Vị trí việc làm - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {final_job}",
                id_=job_id,
                metadata={
                    "type": "ViTriViecLam",
                    ObjectivesProps.LOAI: "CuThe",
                    ObjectivesProps.NOI_DUNG: final_job,
                }
            )
            self.nodes.append(obj_node)

            self._create_relationship(
                source_id=self.program_id,
                target_id=job_id,
                rel_type="CO_CO_HOI_VIEC_LAM"
            )

    def _parse_references(self, text):
        """Trích Tài liệu tham khảo vs Văn bản pháp lý"""
        refs = re.split(r'\n-|\n\+', text)
        std_idx = 1
        for ref in refs:
            normalized_ref = re.sub(r'^[+-]\s*', '', ref, count=1).strip().replace("\n", " ")
            final_ref = re.sub(r'\s+', ' ', normalized_ref)

            is_legal = any(x in final_ref.lower() for x in ["thông tư", "quyết định", "luật", "nghị định", "nghị quyết"])
            # is_standard = any(x in final_ref.lower() for x in ["acm", "aun-qa", "chuẩn", "http"])

            if is_legal:
                so_match = re.search(r'số \s*([^\s\)]+)', final_ref, re.IGNORECASE)
                so = so_match.group(1) if so_match else "Unknown"

                ref_id = self._normalize_id(so)
                data = {
                    "id": "",
                    "so": so,
                    "ten": final_ref,
                    "loai": "Văn bản",
                    "co_quan_ban_hanh": "Không xác định",
                    "ngay_ban_hanh": "",
                    "noi_dung_goc": final_ref
                }

                type_match = re.match(r'^(Quyết định|Thông tư|Nghị định|Luật|Hiến pháp|Chỉ thị|Nghị quyết)', final_ref, re.IGNORECASE)
                if type_match:
                    data["loai"] = type_match.group(1).title()

                date_match = re.search(r'ngày\s+(\d{1,2})\s+tháng\s+(\d{1,2})\s+năm\s+(\d{4})', final_ref, re.IGNORECASE)
                if date_match:
                    d, m, y = date_match.groups()
                    data["ngay_ban_hanh"] = f"{int(d):02d}/{int(m):02d}/{y}"

                agency_pattern = r'của\s+(.+?)\s+(về|ban hành|phê duyệt|quy định|thông qua|ban hành)'
                agency_match = re.search(agency_pattern, final_ref, re.IGNORECASE)

                if agency_match:
                    data["co_quan_ban_hanh"] = agency_match.group(1).strip()
                    start_index = agency_match.end()
                    content_part = final_ref[agency_match.start(2):].strip()
                    data["ten"] = content_part[0].upper() + content_part[1:]
                else:
                    agency_fallback = re.search(r'của\s+(.+)', final_ref, re.IGNORECASE)
                    if agency_fallback:
                        data["co_quan_ban_hanh"] = agency_fallback.group(1).strip()

                vbpl_node = TextNode(
                    text=f"Căn cứ văn bản pháp lý - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {final_ref}",
                    id_=ref_id,
                    metadata={
                        "type": "VanBanPhapLy",
                        LegalDocProps.SO_HIEU: data["so"],
                        LegalDocProps.TEN_VB: data["ten"],
                        LegalDocProps.LOAI_VB: data["loai"],
                        LegalDocProps.NGAY_BAN_HANH: data["ngay_ban_hanh"],
                        LegalDocProps.CO_QUAN: data["co_quan_ban_hanh"],
                        LegalDocProps.NOI_DUNG_GOC: final_ref
                    }
                )
                self.nodes.append(vbpl_node)

                self._create_relationship(
                    source_id=self.program_id,
                    target_id=ref_id,
                    rel_type="TUAN_THU"
                )
            else:
                extracted_link = ""
                rest_content = final_ref

                paren_link_match = re.search(r'\(\s*(https?://[^)]+)\s*\)', final_ref)

                text_to_remove = ""

                if paren_link_match:
                    text_to_remove = paren_link_match.group(0)
                    raw_url = paren_link_match.group(1)

                    extracted_link = re.sub(r'\s+', '', raw_url).strip('.,;?')

                else:
                    m = re.search(r'https?://\S+(?:[\s]\S+)*', final_ref)
                    if m:
                        text_to_remove = m.group(0)
                        extracted_link = re.sub(r'\s+', '', text_to_remove).strip(').,;?')

                clean_full_ref = final_ref

                if text_to_remove:
                    rest_content = final_ref.replace(text_to_remove, "")
                    clean_full_ref = clean_full_ref.replace(text_to_remove, re.sub(r'\s+', '', raw_url))

                rest_content = re.sub(r'\s+', ' ', rest_content)

                rest_content = rest_content.strip(' :.,;')

                ref_id = f"{self.program_id}_ctk_{std_idx}"
                std_idx += 1

                refdoc_node = TextNode(
                    text=f"Chuẩn tham khảo - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {rest_content}",
                    id_=ref_id,
                    metadata={
                        "type": "ChuanThamKhao",
                        RefDocProps.NOI_DUNG: rest_content,
                        RefDocProps.LINK: extracted_link,
                        RefDocProps.NOI_DUNG_GOC: clean_full_ref
                    }
                )
                self.nodes.append(refdoc_node)

                self._create_relationship(
                    source_id=self.program_id,
                    target_id=ref_id,
                    rel_type="THAM_CHIEU"
                )

    def _parse_study_opportunities(self, text):
        """Tạo Node KhaNangHocTap"""
        opps = re.split(r'\n-|\n\+', text)

        for idx, opp in enumerate(opps):
            normalized_opp = re.sub(r'^[+-]\s*', '', opp, count=1).strip().replace("\n", " ")
            final_opp = re.sub(r'\s+', ' ', normalized_opp)

            opp_id = f"{self.program_id}_knht_{idx+1}"

            opp_node = TextNode(
                text=f"Khả năng học tập - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {final_opp}",
                id_=opp_id,
                metadata={
                    "type": "KhaNangHocTap",
                    StudyOpportunitiesProps.NOI_DUNG: final_opp,
                }
            )
            self.nodes.append(opp_node)

            self._create_relationship(
                source_id=self.program_id,
                target_id=opp_id,
                rel_type="TAO_NEN_TANG"
            )

    def _process_learning_outcomes(self, content):
        """
        Xử lý Chuẩn đầu ra (PLOs) tổng quát dựa trên cấu trúc phân cấp.
        Hỗ trợ cả format có PLO và không có PLO (gạch đầu dòng).
        """
        lines = content.split('\n')
        
        # 1. Khởi tạo Trạng thái (Context State)
        current_category = "Chung"          # Cấp 1: Kiến thức / Kỹ năng / Thái độ
        current_subcategory = "Chung"       # Cấp 2: Đại cương / Chuyên ngành
        
        # Bộ đếm để tự sinh ID nếu không tìm thấy mã PLO
        auto_id_counter = 1 
        
        # Regex nhận diện
        # Header cấp 1: Bắt đầu bằng ### hoặc số.số (VD: 2.1, 3.1)
        pat_header_L1 = re.compile(r'^(?:###\s+)?(?:\d+\.\d+\s+)(.*)', re.IGNORECASE)
        
        # Header cấp 2: Bắt đầu bằng #### hoặc số.số.số (VD: 2.1.1, 3.1.2)
        pat_header_L2 = re.compile(r'^(?:####\s+)?(?:\d+\.\d+\.\d+\.?\s+)(.*)', re.IGNORECASE)
        
        # Item liệt kê: Bắt đầu bằng a., b., -, +, *
        pat_list_item = re.compile(r'^(\s*[a-z]\.|-|\+|\*)\s+(.*)')
        
        # Mã PLO: Tìm (PLOxx) hoặc (CDRxx)
        pat_plo_code = re.compile(r'\(((?:PLO|CDR|PO)\d+)\)', re.IGNORECASE)

        for line in lines:
            line = line.strip()
            if not line: continue

            # --- CASE A: Header Cấp 2 (Ưu tiên check trước Cấp 1 vì nó dài hơn) ---
            # VD: #### 3.1.1 Khối kiến thức giáo dục đại cương
            match_l2 = pat_header_L2.match(line)
            if match_l2:
                # Lấy nội dung header, bỏ các ký tự Markdown thừa
                raw_sub = match_l2.group(1).strip('#* ')
                current_subcategory = raw_sub
                continue

            # --- CASE B: Header Cấp 1 ---
            # VD: ### 3.1 Kiến thức
            match_l1 = pat_header_L1.match(line)
            if match_l1:
                raw_cat = match_l1.group(1).strip('#* ')
                current_category = raw_cat
                current_subcategory = "Chung" # Reset cấp con khi sang mục lớn mới
                continue

            # --- CASE C: Nội dung Chuẩn đầu ra (List Item) ---
            match_item = pat_list_item.match(line)
            if match_item:
                content_text = match_item.group(2).strip()
                
                # 1. Tìm Mã PLO (nếu có)
                plo_match = pat_plo_code.search(content_text)
                if plo_match:
                    plo_code = plo_match.group(1).upper()
                    # Xóa mã khỏi nội dung cho sạch
                    clean_content = content_text.replace(plo_match.group(0), "").strip(" ;.,")
                else:
                    # KHÔNG CÓ MÃ PLO -> Tự sinh mã
                    # VD: CDR_01, CDR_02... hoặc sinh theo Category: KT_01
                    # Để đơn giản và duy nhất, ta dùng auto-increment
                    plo_code = f"CDR_{auto_id_counter:02d}"
                    clean_content = content_text
                    auto_id_counter += 1

                # 2. Tạo Node
                # ID Node phải duy nhất toàn cục: ProgramID + Code
                unique_id = f"{self.program_id}_{plo_code}" if self.program_id else f"UNKNOWN_{plo_code}"

                clo_node = TextNode(
                    text=f"Chuẩn đầu ra - Ngành {self.ten_nganh} ({self.loai_hinh_dao_tao}): {clean_content}",
                    id_=self._normalize_id(unique_id),                                                                                                              
                    metadata={
                        "type": "ChuanDauRa",
                        # OutcomeProps.ID: plo_code,
                        OutcomeProps.NOI_DUNG: clean_content,
                        OutcomeProps.NHOM: current_subcategory,                  
                        OutcomeProps.LOAI: current_category,                 
                    }
                )
                self.nodes.append(clo_node)

                # 3. Tạo Quan hệ
                self._create_relationship(
                    source_id=self.program_id,
                    target_id=self._normalize_id(unique_id),
                    rel_type="DAT_CHUAN_DAU_RA"
                )
            else:
                pass

    def generate_embeddings(self):
        """Gom tất cả các Node và thực hiện embedding theo batch"""
        if not self.nodes:
            return

        print(f"--> Đang thực hiện embedding cho {len(self.nodes)} nodes...")
        
        # 1. Chuẩn bị text để embed (kết hợp metadata vào text để search chính xác hơn)
        texts_to_embed = []
        for node in self.nodes:
            texts_to_embed.append(node.text)
        print(len(texts_to_embed))
        # 2. Thực hiện embedding theo batch thông qua Settings.embed_model
        # LlamaIndex sẽ tự động chia nhỏ batch theo cấu hình embed_batch_size
        embeddings = self.embedder.get_embedding_batch(texts_to_embed)
        print(len(embeddings))

        # 3. Gán ngược lại vào từng Node
        for i, node in enumerate(self.nodes):
            node.embedding = embeddings[i]

    def process(self, save_md_path=None):
        """Hàm xử lý chính"""

        print("Đang đọc PDF...")
        raw_md = pymupdf4llm.to_markdown(self.pdf_path)
        
        print("Đang xử lý lại Header...")
        # refined_md = self._upgrade_markdown_headers(raw_md)
        refined = self._upgrade_markdown_headers(raw_md)
        refined_md = self._remove_italic_bold(refined)
        # print(refined_md)
        
        if save_md_path:
            try:
                with open(save_md_path, "w", encoding="utf-8") as f:
                    f.write(refined_md)
                print(f"Đã lưu file Markdown tại: {save_md_path}")
            except Exception as e:
                print(f"Lỗi khi lưu file: {e}")

        print("Đang phân rã Node bằng LlamaIndex...")
        
        parser = MarkdownNodeParser()
        
        input_doc = Document(text=refined_md)
        
        nodes = parser.get_nodes_from_documents([input_doc])
        sections = self._group_nodes(nodes)

        for section in sections:
            # Tách tiêu đề và nội dung
            lines = section.text.split('\n', 1)
            header = lines[0].strip()
            content = lines[1].strip() if len(lines) > 1 else ""

            section_type = self._classify_header(header)
            
            print(f"--> Header: '{header}' ===> Detect: {section_type}")

            if section_type == "THONG_TIN_CHUNG":
                self._parse_general_information(content, refined_md)

            elif section_type == "MUC_TIEU":
                self._parse_program_objectives(content)

            elif section_type == "VI_TRI_VIEC_LAM":
                 self._parse_job_positions(content)

            elif section_type == "KHA_NANG_HOC_TAP":
                 self._parse_study_opportunities(content)

            elif section_type == "THAM_KHAO":
                 self._parse_references(content)
                 
            elif section_type == "KHUNG_CHUONG_TRINH":
                df = extract_curriculum(self.pdf_path)
                self._process_course_table(df)
            
            # if self._has_course_table_signature(content):
            #      print(f"    (Phát hiện bảng môn học ẩn trong mục: {header})")
            #      self._process_course_table(content)
                
            elif section_type == "CHUAN_DAU_RA":
                 print(f"--> Đang xử lý Chuẩn đầu ra: {header}")
                 self._process_learning_outcomes(content)
        
        quyet_dinh = sections[0].text.strip() if sections else ""
        if quyet_dinh:
            self._parse_quyet_dinh_ban_hanh(quyet_dinh, refined_md)

        self.generate_embeddings()

# PDF_FILE = "data/pdf/ChuyenNganh_DaoTao/pdf/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
# etl = CurriculumETL(PDF_FILE)
# nodes = etl.process(save_md_path="debug_output.md")
# nodes = etl.process()
# print(etl.nodes)
# print(etl.edges)