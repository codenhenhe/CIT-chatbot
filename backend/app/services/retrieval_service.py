import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.db.neo4j import get_driver
from app.scripts.Embedding import EmbeddingModel
from app.services.llm_service import call_model_9b

logger = logging.getLogger("app.retrieval")

# --- SCHEMA CHÍNH THỨC CỦA HỆ THỐNG ---
GRAPH_SCHEMA = (
    "Nodes:\n"
    "- ChuongTrinhDaoTao(ma_chuong_trinh, khoa, he, ngon_ngu, tong_tin_chi, thoi_gian_dao_tao, thang_diem): Thực thể trung tâm quản lý chương trình đào tạo.\n"
    "- DoiTuongTuyenSinh(noi_dung): Đối tượng tuyển sinh (VD: Tốt nghiệp THPT hoặc tương đương).\n"
    "- DieuKienTotNghiep(noi_dung): Điều kiện tốt nghiệp (VD: Hoàn thành 120 TC, có điểm trung bình tích lũy tối thiểu 2.0).\n"
    "- HocPhan(ma_hoc_phan, ten_hoc_phan, so_tin_chi, so_tiet_ly_thuyet, so_tiet_thuc_hanh, tom_tat, dieu_kien, yeu_cau_stc_toi_thieu, bat_buoc): Chi tiết về môn học.\n"
    "   + dieu_kien: là học phần điều kiện, không tính điểm trung bình chung tích lũy. Sinh viên có thể hoàn thành các học phần trên bằng hình thức nộp chứng chỉ theo quy định của Trường Đại học Cần Thơ hoặc học tích lũy. \n"
    "   + 'yeu_cau_stc_toi_thieu': Số tín chỉ sinh viên cần tích lũy để được học môn này (VD: 100).\n"
    "   + 'bat_buoc': Giá trị boolean (true/false).\n"
    "- KhoiKienThuc(ma_khoi, ten_khoi, tong_tin_chi, tin_chi_bat_buoc, tin_chi_tu_chon): Các khối như GD đại cương, Cơ sở ngành.\n"
    "- ChuanDauRa(ma_chuan, noi_dung, nhom, loai): Các chuẩn PLO phân theo Kiến thức/Kỹ năng/Mức độ tự chủ và trách nhiệm cá nhân. (VD: nhom: \"Kỹ năng cứng\", loai: \"Kỹ năng\")\n"
    "- VanBanPhapLy(so, ten, loai, ngay_ban_hanh, co_quan_ban_hanh, noi_dung_goc): Quyết định ban hành hoặc văn bản căn cứ.\n"
    "- Nganh(ma_nganh, ten_nganh_vi, ten_nganh_en): Thông tin ngành đào tạo.\n"
    "- Khoa(ma_khoa, ten_khoa): Khoa/Trường.\n"
    "- BoMon(ma_bo_mon, ten_bo_mon): Bộ môn.\n"
    "- TrinhDo(ma_trinh_do, ten_trinh_do): Cấp bậc đào tạo (Đại học/Cao đẳng/Thạc sĩ/Tiến sĩ).\n"
    "- LoaiVanBang(ma_loai, loai_van_bang): Danh hiệu văn bằng (Kỹ sư/Cử nhân/Thạc sĩ/Tiến sĩ).\n"
    "- HinhThucDaoTao(ma_hinh_thuc, ten_hinh_thuc): Hình thức đào tạo (Chính quy/Vừa làm vừa học/Đào tạo từ xa).\n"
    "- PhuongThucDaoTao(ma_phuong_thuc, ten_phuong_thuc): Phương thức đào tạo (Trực tiếp/Trực tuyến).\n"
    "- MucTieuDaoTao(loai, noi_dung): Mục tiêu PEOs chung hoặc cụ thể.\n"
    "- ViTriViecLam(noi_dung): Cơ hội nghề nghiệp sau tốt nghiệp.\n"
    "- ChuanThamKhao(noi_dung, link, noi_dung_goc): Tài liệu hoặc chuẩn tham chiếu quốc tế.\n"
    "- DanhGiaKiemDinh(noi_dung): Thông tin về đánh giá và kiểm định chất lượng chương trình đào tạo.\n"
    "- KhaNangHocTap(noi_dung): Khả năng học tập sau tốt nghiệp.\n"
    "- NhomHocPhanTuChon(ten_nhom): Nhóm nhỏ môn học trong yêu cầu tự chọn. (VD: AV, PV)\n"
    "- YeuCauTuChon(noi_dung_yeu_cau, so_tin_chi_yeu_cau): Ràng buộc nhóm học phần tự chọn và số tín chỉ tự chọn (VD:, N1 hoặc N2, Chọn 10 TC).\n\n"

    "Edges:\n"
    "- (:ChuongTrinhDaoTao)-[:BAN_HANH_THEO]->(:VanBanPhapLy)\n"
    "- (:ChuongTrinhDaoTao)-[:DAO_TAO]->(:TrinhDo)\n"
    "- (:ChuongTrinhDaoTao)-[:THUOC_VE]->(:Nganh)\n"
    "- (:Nganh)-[:THUOC_VE]->(:Khoa|:BoMon)\n"
    "- (:BoMon)-[:THUOC_VE]->(:Khoa)\n"
    "- (:ChuongTrinhDaoTao)-[:CAP]->(:LoaiVanBang)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:HinhThucDaoTao)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:PhuongThucDaoTao)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:DoiTuongTuyenSinh)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:MucTieuDaoTao)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:DanhGiaKiemDinh)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:ViTriViecLam)\n"
    "- (:ChuongTrinhDaoTao)-[:THAM_KHAO]->(:ChuanThamKhao)\n"
    "- (:ChuongTrinhDaoTao)-[:DAT_DUOC]->(:KhaNangHocTap)\n"
    "- (:ChuongTrinhDaoTao)-[:CO]->(:ChuanDauRa)\n"
    "- (:ChuongTrinhDaoTao)-[:YEU_CAU]->(:DieuKienTotNghiep)\n"
    "- (:ChuongTrinhDaoTao)-[:GOM]->(:KhoiKienThuc)\n"
    "- (:ChuongTrinhDaoTao)-[:GOM]->(:HocPhan)\n"
    "- (:KhoiKienThuc)-[:GOM]->(:HocPhan)\n"
    "- (:YeuCauTuChon)-[:DOI_VOI]->(:NhomHocPhanTuChon)\n"
    "- (:YeuCauTuChon)-[:GOM]->(:HocPhan)\n"
    "- (:NhomHocPhanTuChon)-[:GOM]->(:HocPhan)\n"
    "- (:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(:HocPhan)\n"
    "- (:HocPhan)-[:CO_THE_SONG_HANH]->(:HocPhan)\n"
)

FEW_SHOT_EXAMPLES = """
User: "Môn Kiến trúc máy tính bao nhiêu tín chỉ?"
Cypher: MATCH (h:HocPhan) WHERE toLower(h.ten_hoc_phan) CONTAINS toLower('Kiến trúc máy tính') RETURN h.ten_hoc_phan, h.so_tin_chi LIMIT 5

User: "Học phần tiên quyết của môn Lập trình hướng đối tượng là gì?"
Cypher: MATCH (h_pre:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(h:HocPhan) WHERE toLower(h.ten_hoc_phan) CONTAINS toLower('Lập trình hướng đối tượng') RETURN h_pre.ten_hoc_phan AS hoc_phan_tien_quyet LIMIT 10

User: "Ngành Kỹ thuật phần mềm thuộc khoa nào?"
Cypher: MATCH (n:Nganh)-[:THUOC_VE]->(k:Khoa) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Kỹ thuật phần mềm') RETURN n.ten_nganh_vi, k.ten_khoa LIMIT 5

User: "Khóa 50 có những ngành nào và tổng tín chỉ bao nhiêu?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao {khoa: 50})-[:THUOC_VE]->(n:Nganh) RETURN n.ten_nganh_vi, ctdt.tong_tin_chi LIMIT 20

User: "Liệt kê các môn trong khối chuyên ngành"
Cypher: MATCH (k:KhoiKienThuc)-[:GOM]->(h:HocPhan) WHERE toLower(k.ten_khoi) CONTAINS toLower('chuyên ngành') RETURN h.ten_hoc_phan, h.so_tin_chi LIMIT 30

User: "Chuẩn đầu ra kỹ năng của ngành Mạng máy tính là gì?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh), (ctdt)-[:CO]->(cdr:ChuanDauRa) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Mạng máy tính') AND toLower(cdr.loai) CONTAINS toLower('kỹ năng') RETURN cdr.noi_dung LIMIT 20

User: "Chuẩn đầu ra về ngoại ngữ của ngành Mạng máy tính là gì?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh), (ctdt)-[:CO]->(cdr:ChuanDauRa) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Mạng máy tính') AND toLower(cdr.noi_dung) CONTAINS toLower('ngoại ngữ') RETURN cdr.noi_dung LIMIT 20

User: "Thông tin về chương trình đào tạo ngành Khoa học máy tính khóa 51?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
        WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Khoa học máy tính') 
        AND ctdt.khoa = 51
        RETURN n.ten_nganh_vi AS nganh, 
            ctdt.he AS he_dao_tao, 
            ctdt.tong_tin_chi, 
            ctdt.thoi_gian_dao_tao,
            ctdt.ngon_ngu AS ngon_ngu_giang_day,
            ctdt.thang_diem AS thang_diem_danh_gia
        LIMIT 10

User: "Thông tin về chương trình đào tạo ngành Công nghệ thông tin?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)
        WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Công nghệ thông tin')
        RETURN n.ten_nganh_vi AS nganh, 
            ctdt.khoa AS khoa,
            ctdt.he AS he_dao_tao, 
            ctdt.tong_tin_chi, 
            ctdt.thoi_gian_dao_tao,
            ctdt.ngon_ngu AS ngon_ngu_giang_day,
            ctdt.thang_diem AS thang_diem_danh_gia
        ORDER BY ctdt.khoa DESC 
        LIMIT 10
"""

# --- UTILITIES ---
_embed_model: Optional[EmbeddingModel] = None
_embed_model_lock = threading.Lock()
_embedding_cache: Dict[str, List[float]] = {}
_source_metrics = {"requests": 0, "graph": 0, "vector": 0, "none": 0, "start": time.time()}


def _truncate_for_log(text: str, limit: int = 500) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    return raw[:limit] + f"...[+{len(raw) - limit} chars]"


def _to_log_json(obj: Any, limit: int = 1000) -> str:
    text = json.dumps(obj, ensure_ascii=False)
    if len(text) <= limit:
        return text
    return text[:limit] + f"...[+{len(text) - limit} chars]"


def _log_metrics(source: str) -> None:
    _source_metrics["requests"] += 1
    if source in _source_metrics:
        _source_metrics[source] += 1


def _get_embed_model() -> EmbeddingModel:
    global _embed_model
    if _embed_model:
        return _embed_model
    with _embed_model_lock:
        if _embed_model:
            return _embed_model
        _embed_model = EmbeddingModel()
        return _embed_model


def _is_safe_cypher(cypher: str) -> bool:
    cypher = (cypher or "").strip()
    if not cypher:
        return False
    dangerous = ("DROP", "DELETE", "CREATE", "ALTER", "TRUNCATE", ";--", "*/")
    return not any(keyword in cypher.upper() for keyword in dangerous)


def _humanize_column_name(col_name: str) -> str:
    mapping = {
        "ma_hoc_phan": "Mã HP",
        "ten_hoc_phan": "Tên HP",
        "so_tin_chi": "Tín chỉ",
        "so_tiet_ly_thuyet": "Tiết LT",
        "so_tiet_thuc_hanh": "Tiết TH",
        "tom_tat": "Tóm tắt",
        "dieu_kien": "Điều kiện",
        "yeu_cau_stc_toi_thieu": "STC tối thiểu",
        "bat_buoc": "Bắt buộc",
        "ma_nganh": "Mã ngành",
        "ten_nganh_vi": "Tên ngành VN",
        "ten_nganh_en": "Tên ngành EN",
        "ma_khoa": "Mã khoa",
        "ten_khoa": "Tên khoa",
        "ma_khoi": "Mã khối",
        "ten_khoi": "Tên khối",
        "khoa": "Khóa",
        "he": "Hệ",
        "tong_tin_chi": "Tổng tín chỉ",
    }
    return mapping.get(col_name, col_name.replace("_", " ").title())


# --- CORE LOGIC ---

async def generate_cypher_with_llm(question: str) -> str:
    """Sử dụng 9B để biến ngôn ngữ tự nhiên thành Cypher dựa trên Schema chi tiết."""
    prompt = f"""
Bạn là chuyên gia Neo4j cho hệ thống đào tạo Đại học Cần Thơ.
Dựa trên Schema và các ví dụ đúng schema sau, hãy viết câu lệnh Cypher để trả lời câu hỏi.

SCHEMA:
{GRAPH_SCHEMA}

## CHÚ Ý ĐẶC BIỆT - DỄ BỊ NHẦM LẬN - PHẢI ĐỌC KỸ:

### 1. ChuongTrinhDaoTao.khoa = KHÓA TUYỂN SINH (từ: năm nhập học), KHÔNG phải Khoa đơn vị
   - Ý: khoa: 50 = chương trình dành cho sinh viên khóa 50 (năm 2050 tuyển)
   - khoa là SỐ NGUYÊN, KHÔNG edge [THUOC_VE] tới Khoa entity nào
   - Nếu câu hỏi hỏi "Khóa 51 có những ngành nào" -> Dùng {{khoa: 51}}

### 2. HocPhan.dieu_kien = MÔN ĐIỀU KIỆN (không tính GPA), KHÔNG phải tiên quyết
   - Tiên quyết: HocPhan A --[YEU_CAU_TIEN_QUYET]--> HocPhan B
   - Điều kiện: dieu_kien=true/false (boolean), học phần này nhưng không tính điểm TB
   - Nếu hỏi "tiên quyết của môn X" -> Dùng YEU_CAU_TIEN_QUYET relationship

### 3. HocPhan.yeu_cau_stc_toi_thieu = STC TÍCH LŨY TỐI THIỂU (để được học), KHÔNG phải STC của môn
   - STC của môn = so_tin_chi
   - STC tựu lũy tối thiểu = yeu_cau_stc_toi_thieu (VD: phải tích lũy 100 STC mới học được)
   - Nếu hỏi "môn này bao nhiêu tín chỉ" -> RETURN so_tin_chi
   - Nếu hỏi "đầy đủ bao nhiêu tín chỉ mới được học" -> RETURN yeu_cau_stc_toi_thieu

### 4. HinhThucDaoTao vs PhuongThucDaoTao - ba khái niệm khác nhau
   - **HinhThucDaoTao**: Hình thức (Chính quy, Vừa làm vừa học, Từ xa)
   - **PhuongThucDaoTao**: Phương thức (Trực tiếp, Trực tuyến)
   - Nếu hỏi "hệ CQ" -> WHERE toLower(h.he) CONTAINS 'CQ'
   - Nếu hỏi "chính quy" -> Tìm HinhThucDaoTao với ten_hinh_thuc chứa 'Chính quy'

### 5. HocPhan.bat_buoc = BẮT BUỘC HỌC (boolean), KHÔNG phải bắt buộc tính điểm
   - bat_buoc=true: Phải học, có thể là điều kiện hoặc bắt buộc tính điểm
   - bat_buoc=false: Môn tự chọn, có thể không học nếu trong KhoiKienThuc tự chọn
   - Nếu hỏi "các môn bắt buộc" -> WHERE h.bat_buoc=true

### 6. DieuKienTotNghiep vs YeuCauTuChon - Hai loại yêu cầu khác nhau
   - **DieuKienTotNghiep**: Điều kiện CHUNG (phải hoàn thành) -> ChuongTrinhDaoTao --[YEU_CAU]--> DieuKienTotNghiep
   - **YeuCauTuChon**: Yêu cầu RIÊNG cho tự chọn -> YeuCauTuChon --[DOI_VOI]--> NhomHocPhanTuChon
   - Nếu hỏi "điều kiện tốt nghiệp là gì" -> YEU_CAU edge
   - Nếu hỏi "yêu cầu tự chọn như thế nào" -> YeuCauTuChon và NhomHocPhanTuChon

VÍ DỤ:
{FEW_SHOT_EXAMPLES}

HƯỚNG DẪN QUAN TRỌNG:
1. Chỉ trả về duy nhất câu lệnh Cypher. KHÔNG GIẢI THÍCH GÌ.
2. LƯU Ý VỀ KIỂU DỮ LIỆU:
   - Thuộc tính "khoa" là SỐ NGUYÊN (VD: 51, 50, 52) → Không dùng ngoặc đơn. Ví dụ: {{khoa: 51}}
    - Thuộc tính chuỗi (he, ten_hoc_phan, ten_nganh_vi, ten_khoi...) → Dùng ngoặc đơn.
3. Luôn sử dụng toLower() cho các thuộc tính tên để tìm kiếm linh hoạt.
4. Chỉ dùng quan hệ và thuộc tính có trong schema hiện tại.
5. Không thêm dấu ngoặc nhọn xung quanh câu trả lời.

Câu hỏi: {question}
Cypher:"""
    
    try:
        # Temperature thấp để tăng tính ổn định của mẫu Cypher.
        raw_cypher = await call_model_9b(prompt, temperature=0.2)
        # Làm sạch kết quả trả về từ LLM
        clean_cypher = raw_cypher.strip().replace("```cypher", "").replace("```", "").split(";")[0]
        logger.info("[retrieval][cypher] generated len=%s cypher=%s", len(clean_cypher), _truncate_for_log(clean_cypher, 360))
        return clean_cypher
    except Exception as e:
        logger.error(f"[retrieval][cypher] error: {e}")
        return ""


def _execute_query(cypher: str) -> List[Dict]:
    """Thực thi Cypher query trên Neo4j."""
    try:
        driver = get_driver()
        with driver.session() as session:
            result = session.run(cypher, {})
            rows = result.data()
            logger.info("[retrieval][graph][cypher] %s", _truncate_for_log(cypher, 1200))
            logger.info("[retrieval][graph][rows] count=%s rows=%s", len(rows), _to_log_json(rows, 8000))
            return rows
    except Exception as e:
        logger.error(f"[retrieval][graph] error: {e}")
        return []


def _execute_query_with_params(cypher: str, params: dict) -> List[Dict]:
    """Thực thi Cypher query với parameters."""
    try:
        driver = get_driver()
        with driver.session() as session:
            rows = session.run(cypher, **params).data()
            logger.info("[retrieval][vector][cypher] %s", _truncate_for_log(cypher, 800))
            logger.info("[retrieval][vector][rows] count=%s rows=%s", len(rows), _to_log_json(rows, 4000))
            return rows
    except Exception as e:
        logger.error(f"[retrieval][vector] error: {e}")
        return []


async def _get_vector_context(question: str, top_k: int = 2) -> str:
    """
    Vector search với Trust Hierarchy thấp hơn Graph:
    - Score threshold: >= 0.7 (tin cậy cao)
    - Top-K giới hạn: 2 kết quả (tránh nhiễu)
    """
    try:
        model = _get_embed_model()
        vec = await asyncio.to_thread(model.get_embedding_batch, [question])
        if not vec:
            return ""
        
        cypher_vector = """
        CALL db.index.vector.queryNodes('global_knowledge_index', $top_k, $vector)
        YIELD node, score WHERE score >= 0.7
        RETURN coalesce(node.text, node.noi_dung, node.ten_hp) as txt, score
        """
        rows = await asyncio.to_thread(
            _execute_query_with_params, 
            cypher_vector, 
            {"vector": vec[0], "top_k": max(top_k, 1)}
        )
        
        if not rows:
            return ""
        
        # Format results
        formatted = []
        for row in rows[:top_k]:
            txt = row.get('txt', '')
            score = row.get('score', 0)
            if txt:
                formatted.append(f"- {txt} (score: {score:.2f})")

        if formatted:
            logger.info(
                "[retrieval][vector][formatted] top_k=%s content=%s",
                top_k,
                _to_log_json(formatted, 4000),
            )
        
        return "\n".join(formatted) if formatted else ""
    except Exception as e:
        logger.error(f"[retrieval][vector] error: {e}")
        return ""


async def retrieve_graph_context(question: str, top_k: int = 4) -> str:
    """
    Graph-First with Semantic Fallback Pipeline
    
    Trust Hierarchy (Thứ bậc tin cậy):
    1. Graph Results (100% trust) - Ưu tiên đầu tiên
    2. Vector Results (60-80% trust) - Dự phòng khi Graph rỗng
    """
    logger.info("[retrieval][input] question=%s", _truncate_for_log(question, 220))
    
    # ======== STEP 1: Graph-First ========
    graph_context = ""
    cypher = await generate_cypher_with_llm(question)
    
    if _is_safe_cypher(cypher):
        rows = await asyncio.to_thread(_execute_query, cypher)
        if rows:
            # Format Graph results with human-readable labels
            formatted_rows = []
            for idx, row in enumerate(rows[:top_k], 1):
                row_items = []
                for k, v in row.items():
                    human_label = _humanize_column_name(k)
                    row_items.append(f"{human_label}: {v}")
                formatted_rows.append(f"{idx}. {' | '.join(row_items)}")
            
            # Add context hint from Cypher filters
            context_hint = ""
            if "{khoa:" in cypher:
                import re
                khoa_match = re.search(r'\{khoa:\s*(\d+)\}', cypher)
                if khoa_match:
                    khoa_num = khoa_match.group(1)
                    context_hint = f"[Khóa {khoa_num}] "
            
            graph_context = f"DỮ LIỆU CẤU TRÚC (Độ tin cậy 100%) {context_hint}:\n" + "\n".join(formatted_rows)
            logger.info("[retrieval][graph_hit] context_len=%s", len(graph_context))
            _log_metrics("graph")
    
    # ======== STEP 2: Semantic Fallback ========
    vector_context = ""
    if not graph_context:
        # Only query Vector if Graph returns nothing
        vector_result = await _get_vector_context(question, top_k=2)
        if vector_result:
            vector_context = f"DỮ LIỆU TỬ VẤN VĂN BẢN (Độ tin cậy 60-80%) :\n{vector_result}"
            logger.info("[retrieval][vector_fallback] context_len=%s", len(vector_context))
            _log_metrics("vector")
        else:
            _log_metrics("none")
    else:
        _log_metrics("graph")
    
    # ======== STEP 3: Combine with Trust Hierarchy ========
    final_context = ""
    if graph_context:
        final_context = graph_context
        if vector_context:
            final_context += "\n\n" + vector_context
    else:
        final_context = vector_context if vector_context else "Hệ thống không tìm thấy dữ liệu liên quan."
    
    logger.info("[retrieval][final] context_len=%s source=%s", len(final_context), "graph" if graph_context else ("vector" if vector_context else "none"))
    
    return final_context.strip()


def warmup_embedding_model() -> bool:
    """Khởi động model embedding để tránh độ trễ (cold-start) ở request đầu tiên."""
    logger.info("[retrieval] Bắt đầu warmup embedding model...")
    try:
        model = _get_embed_model()
        vectors = model.get_embedding_batch(["warmup_query_ctu"])
        if vectors:
            _embedding_cache["warmup_query_ctu"] = vectors[0]
        logger.info("[retrieval] Warmup embedding model thành công!")
        return True
    except Exception as e:
        logger.error(f"[retrieval] Lỗi khi warmup embedding model: {e}")
        return False


@dataclass
class RetrievalResult:
    strategy: str
    context: str
    sources: List[str]


class RetrievalService:
    """Retrieval service with Graph-First strategy."""

    def __init__(self) -> None:
        self.cache_ttl_seconds = int(os.getenv("RETRIEVAL_CACHE_TTL_SECONDS", "180"))
        self._cache: Dict[str, Tuple[float, RetrievalResult]] = {}

    async def execute(
        self,
        strategy: str,
        query: str,
        entities: Dict[str, str],
        top_k: int = 5,
    ) -> RetrievalResult:
        """
        Execute retrieval using Graph-First with Semantic Fallback.
        
        strategy parameter is deprecated - all use Graph-First approach now.
        """
        strategy = (strategy or "hybrid").lower()
        cache_key = self._build_cache_key(strategy, query, entities, top_k)
        cached = self._get_cached(cache_key)
        if cached:
            logger.info("[retrieval][cache] hit strategy=%s", strategy)
            return cached

        logger.info("[retrieval][step] execute strategy=%s top_k=%s", strategy, top_k)

        # All strategies use Graph-First now
        try:
            context = await retrieve_graph_context(query, top_k)
            result = RetrievalResult("graph-first", context, ["neo4j", "vector"])
            logger.info("[retrieval][result] context_len=%s", len(context))
            self._set_cached(cache_key, result)
            return result
        except Exception as exc:
            logger.error("[retrieval] execute failed: %s", exc)
            return RetrievalResult("error", "", [])

    @staticmethod
    def _build_cache_key(strategy: str, query: str, entities: Dict[str, str], top_k: int) -> str:
        payload = {
            "strategy": strategy,
            "query": (query or "").strip(),
            "entities": entities or {},
            "top_k": int(top_k),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _get_cached(self, key: str) -> Optional[RetrievalResult]:
        item = self._cache.get(key)
        if not item:
            return None
        ts, value = item
        if (time.time() - ts) > self.cache_ttl_seconds:
            self._cache.pop(key, None)
            return None
        return value

    def _set_cached(self, key: str, value: RetrievalResult) -> None:
        self._cache[key] = (time.time(), value)
        if len(self._cache) > 1000:
            self._cleanup_cache()

    def _cleanup_cache(self) -> None:
        now = time.time()
        expired = [k for k, (ts, _) in self._cache.items() if (now - ts) > self.cache_ttl_seconds]
        for k in expired:
            self._cache.pop(k, None)
