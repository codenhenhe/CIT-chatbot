from enum import Enum

# ĐỊNH NGHĨA NODE 

class NodeType(str, Enum):
    """Định nghĩa các Label cho Node trong Neo4j"""
    BO_MON = "BoMon"                            # Bộ môn
    KHOA = "Khoa"                               # Khoa
    HOC_PHAN = "HocPhan"                        # Học phần
    NGANH = "Nganh"                             # Ngành đào tạo
    CHUONG_TRINH_DAO_TAO = "ChuongTrinhDaoTao"  # Chương trình đào tạo
    VAN_BAN_PHAP_LY = "VanBanPhapLy"            # Văn bản pháp lý
    VI_TRI_VIEC_LAM = "ViTriViecLam"            # Vị trí việc làm
    MUC_TIEU_DAO_TAO = "MucTieuDaoTao"          # PEOs
    CHUAN_DAU_RA = "ChuanDauRa"                 # PLOs
    KHA_NANG_HOC_TAP = "KhaNangHocTap"          # Khả năng học tập
    KHOI_KIEN_THUC = "KhoiKienThuc"             # Khối kiến thức
    CHUAN_THAM_KHAO = "ChuanThamKhao"           # Tài liệu tham khảo
    
# ĐỊNH NGHĨA THUỘC TÍNH 

class FacultyProps(str, Enum):
    """Thuộc tính cho Node KHOA (Khoa)"""
    TEN = "ten_khoa"            

class DepartmentProps(str, Enum):
    """Thuộc tính cho Node Bộ môn (BoMon)"""
    TEN = "ten_bomon"   
             
class MajorProps(str, Enum):
    """Thuộc tính cho Node Ngành (Nganh)"""
    TEN_VI = "ten_nganh_vi"
    TEN_EN = "ten_nganh_en"

class ProgramProps(str, Enum):
    """Thuộc tính cho Node Chương trình đào tạo (ChuongTrinhDaoTao)"""
    MA_CHUONG_TRINH = "ma_chuong_trinh"                       # Mã chương trình (VD: 7480202_STD", "7480202_CLC")
    KHOA_HOC = "khoa"               # Khóa (VD: 48)
    LOAI_HINH = "loai_hinh"         # Chất lượng cao / Đại trà
    NGON_NGU = "ngon_ngu"           # Tiếng Việt / Anh
    TONG_TIN_CHI = "tong_tin_chi"   # 161
    THOI_GIAN = "thoi_gian_dao_tao" # 4.5

class DegreeProps(str, Enum):
    """Thuộc tính cho Node Loại văn bằng (LoaiVanBang)"""
    ID = "id"
    TEN = "loai_van_bang" # Cử nhân / Kỹ sư

class LevelProps(str, Enum): # Đại học, Thạc sĩ, Tiến sĩ
    """Thuộc tính cho Node Trình độ (TrinhDo)"""
    ID = "id"
    TEN = "ten_trinh_do"

class TrainingFormProps(str, Enum): 
    """Thuộc tính cho Node Hình thức đào tạo (HinhThucDaoTao)"""
    ID = "id"
    TEN = "ten_hinh_thuc" # Chính quy / Vừa làm vừa học / Đào tạo từ xa

class TrainingMethodProps(str, Enum):
    """Thuộc tính cho Node Phương thức (PhuongThucDaoTao)"""
    ID = "id"
    TEN = "ten_phuong_thuc" # Trực tiếp / Trực tuyến

class CourseProps(str, Enum):
    """Thuộc tính cho Node Học phần (HocPhan)"""
    ID = "id"
    TEN = "ten" 
    SO_TIN_CHI = "so_tin_chi"
    SO_TIET_LY_THUYET = "so_tiet_ly_thuyet"
    SO_TIET_THUC_HANH = "so_tiet_thuc_hanh"
    BAT_BUOC = "bat_buoc" # true/false

class ObjectivesProps(str, Enum):
    """Thuộc tính cho Node Mục tiêu đào tạo (MucTieuDaoTao)"""
    ID = "id"                   # 7480202_51_STD_PEO1
    LOAI = "loai"               # chung / cu_the
    NOI_DUNG = "noi_dung"       # Kiến thức cơ bản về lý luận chính trị....

class LegalDocProps(str, Enum):
    """Thuộc tính cho Node Văn bản pháp lý (VanBanPhapLy)"""
    ID = "id"                       # 1982_QD_TTg
    SO_HIEU = "so"                  # 1982/QĐ-TTg
    TEN_VB = "ten"                  # Phê duyệt Khung trình độ quốc gia Việt Nam
    LOAI_VB = "loai"                # Quyết định
    CO_QUAN = "co_quan_ban_hanh"    # Thủ tướng Chính phủ
    NGAY_BAN_HANH = "ngay_ban_hanh" # 18/10/2016
    NOI_DUNG_GOC = "noi_dung_goc"   # Nội dung gốc văn bản

class OutcomeProps(str, Enum):
    """Thuộc tính cho Node Chuẩn đầu ra (ChuanDauRa)"""
    ID = "id"                      # 7480202_51_STD_PLO1
    NHOM = "nhom"                  # Kiến thức / Kỹ năng / Thái độ / Khác
    LOAI = "loai"                  
    NOI_DUNG = "noi_dung"          # Áp dụng được các kiến thức toán học, khoa học tự nhiên...

class RefDocProps(str, Enum):
    """Thuộc tính cho Node Chuẩn tham khảo (ChuanThamKhao)"""
    ID = "id"                      # 7480202_51_STD_ctk_1      
    NOI_DUNG = "noi_dung"          # Tiêu chuẩn đánh giá chất lượng cấp chương trình đào tạo của AUN-QA (phiên bản 2020)
    LINK = "link"
    NOI_DUNG_GOC = "noi_dung_goc"
    # EMBEDDING = "embedding"        # Vector embeddings noi_dung

class StudyOpportunitiesProps(str, Enum):
    """Thuộc tính cho Node Khả năng học tập (KhaNangHocTap)"""
    MA_KHA_NANG = "ma_kha_nang"         # 7480202_51_STD_knht_1
    NOI_DUNG = "noi_dung"               # Sinh viên có khả năng học tập tiếp các chương trình sau đại học trong và ngoài nước

class JobPosProps(str, Enum):
    """Thuộc tính cho Node Vị trí việc làm (ViTriViecLam)"""
    MA_VI_TRI = "ma_vi_tri"             # 7480202_51_STD_vtvl_1
    NOI_DUNG = "noi_dung"               # Sinh viên có cơ hội nghề nghiệp trong các công ty tư ...

class CourseGroupProps(str, Enum):
    """Thuộc tính cho Node Khối kiến thức (KhoiKienThuc)"""
    MA_KHOI = "ma_khoi"            # Mã khối
    TEN = "ten"                    # Khối kiến thức giáo dục đại cương

# ĐỊNH NGHĨA QUAN HỆ (EDGES) 

class RelType(str, Enum):
    THUOC_KHOA = "THUOC_KHOA"         # BoMon -> Khoa
    THUOC_NGANH = "THUOC_NGANH"       # ChuongTrinh -> Nganh
    
    # Quan hệ chứa (Hierarchy)
    HAS_SECTION = "HAS_SECTION"
    
    # Quan hệ chương trình
    BAO_GOM_MON = "BAO_GOM_MON"       # ChuongTrinh -> HocPhan
    DAT_MUC_TIEU = "DAT_MUC_TIEU"     # ChuongTrinh -> MucTieu
    DAT_CHUAN_DAU_RA = "DAT_CHUAN_DAU_RA" # ChuongTrinh -> ChuanDauRa
    
    # Quan hệ môn học
    TIEN_QUYET = "TIEN_QUYET"
    SONG_HANH = "SONG_HANH"
    THUOC_KHOI = "THUOC_KHOI"         # HocPhan -> KhoiKienThuc
    
    # Quan hệ pháp lý
    CAN_CU_PHAP_LY = "CAN_CU_PHAP_LY" # ChuongTrinh -> VanBanPhapLy
    THAM_CHIEU = "THAM_CHIEU"         # ChuongTrinh -> ChuanThamKhao