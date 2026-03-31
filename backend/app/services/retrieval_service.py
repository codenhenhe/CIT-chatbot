import asyncio
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import re

from app.db.neo4j import get_driver
from app.scripts.Embedding import EmbeddingModel
from app.services.llm_service import call_model_7b

logger = logging.getLogger("app.retrieval")

# --- SCHEMA CHÍNH THỨC CỦA HỆ THỐNG ---
GRAPH_SCHEMA = (
            "Nodes:\n"
            "- ChuongTrinhDaoTao(id, ma_chuong_trinh, khoa, loai_hinh, ngon_ngu, tong_tin_chi, thoi_gian_dao_tao): Thực thể trung tâm quản lý chương trình đào tạo.\n"
            "- HocPhan(id, ten, so_tin_chi, so_tiet_ly_thuyet, so_tiet_thuc_hanh, bat_buoc): Chi tiết về môn học.\n"
            "- KhoiKienThuc(id, ma_khoi, ten, tong_tin_chi, tin_chi_bat_buoc, tin_chi_tu_chon): Các khối như GD đại cương, Cơ sở ngành.\n"
            "- YeuCauTuChon(id, noi_dung_yeu_cau, mo_ta): Ràng buộc số tín chỉ tự chọn (VD: Chọn 10 TC).\n"
            "- NhomHocPhanTuChon(id, ten_nhom): Nhóm nhỏ môn học trong yêu cầu tự chọn.\n"
            "- ChuanDauRa(id, noi_dung, nhom, loai): Các chuẩn PLO/CDR phân theo Kiến thức/Kỹ năng/Thái độ.\n"
            "- VanBanPhapLy(id, so, ten, loai, ngay_ban_hanh, co_quan_ban_hanh, noi_dung_goc): Quyết định ban hành hoặc văn bản căn cứ.\n"
            "- Nganh(id, ten_nganh_vi, ten_nganh_en): Thông tin ngành đào tạo.\n"
            "- Khoa(id, ten_khoa): Khoa/Trường.\n"
            "- BoMon(id, ten_bomon): Bộ môn.\n"
            "- TrinhDo(id, ten_trinh_do): Cấp bậc đào tạo (Đại học/Cao đẳng/Thạc sĩ/Tiến sĩ).\n"
            "- LoaiVanBang(id, loai_van_bang): Danh hiệu văn bằng (Kỹ sư/Cử nhân/Thạc sĩ/Tiến sĩ).\n"
            "- HinhThucDaoTao(id, ten_hinh_thuc): Hình thức đào tạo (Chính quy/Vừa làm vừa học/Đào tạo từ xa).\n"
            "- PhuongThucDaoTao(id, ten_phuong_thuc): Phương thức đào tạo (Trực tiếp/Trực tuyến).\n"
            "- MucTieuDaoTao(id, loai, noi_dung): Mục tiêu PEOs chung hoặc cụ thể.\n"
            "- ViTriViecLam(id, ma_vi_tri, noi_dung): Cơ hội nghề nghiệp sau tốt nghiệp.\n"
            "- ChuanThamKhao(id, noi_dung, link, noi_dung_goc): Tài liệu hoặc chuẩn tham chiếu quốc tế.\n"
            "- KhaNangHocTap(id, ma_kha_nang, noi_dung): Định hướng học tập nâng cao.\n\n"

            "Edges:\n"
            "- (:ChuongTrinhDaoTao)-[:DUOC_BAN_HANH_THEO]->(:VanBanPhapLy)\n"
            "- (:ChuongTrinhDaoTao)-[:DAO_TAO_TRINH_DO]->(:TrinhDo)\n"
            "- (:ChuongTrinhDaoTao)-[:THUOC_VE]->(:Nganh)\n"
            "- (:Nganh)-[:THUOC_VE]->(:Khoa|:BoMon)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_LOAI_VAN_BANG]->(:LoaiVanBang)\n"
            "- (:ChuongTrinhDaoTao)-[:DAO_TAO_THEO_HINH_THUC]->(:HinhThucDaoTao)\n"
            "- (:ChuongTrinhDaoTao)-[:DAO_TAO_THEO_PHUONG_THUC]->(:PhuongThucDaoTao)\n"
            "- (:PhuongThucDaoTao)-[:CO_QUY_DINH]->(:VanBanPhapLy)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_MUC_TIEU_DAO_TAO]->(:MucTieuDaoTao)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_CO_HOI_VIEC_LAM]->(:ViTriViecLam)\n"
            "- (:ChuongTrinhDaoTao)-[:TUAN_THU]->(:VanBanPhapLy)\n"
            "- (:ChuongTrinhDaoTao)-[:THAM_CHIEU]->(:ChuanThamKhao)\n"
            "- (:ChuongTrinhDaoTao)-[:TAO_NEN_TANG]->(:KhaNangHocTap)\n"
            "- (:ChuongTrinhDaoTao)-[:DAT_CHUAN_DAU_RA]->(:ChuanDauRa)\n"
            "- (:ChuongTrinhDaoTao)-[:CO_KHOI_KIEN_THUC]->(:KhoiKienThuc)\n"
            "- (:KhoiKienThuc)-[:CO_YEU_CAU_TU_CHON]->(:YeuCauTuChon)\n"
            "- (:YeuCauTuChon)-[:CO_NHOM_THANH_PHAN]->(:NhomHocPhanTuChon)\n"
            "- (:KhoiKienThuc|:YeuCauTuChon|:NhomHocPhanTuChon)-[:GOM_HOC_PHAN]->(:HocPhan)\n"
            "- (:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(:HocPhan)\n"
            "- (:HocPhan)-[:CO_THE_SONG_HANH]->(:HocPhan)\n"
        )

FEW_SHOT_EXAMPLES = """
User: "Môn Kỹ thuật lập trình bao nhiêu tín chỉ?"
Cypher: MATCH (h:HocPhan) WHERE toLower(h.ten) CONTAINS toLower('Kỹ thuật lập trình') RETURN h.ten, h.so_tin_chi LIMIT 5

User: "Cơ hội việc làm của ngành Công nghệ thông tin?"
Cypher: MATCH (n:Nganh)<-[:THUOC_VE]-(ctdt:ChuongTrinhDaoTao)-[:CO_CO_HOI_VIEC_LAM]->(v:ViTriViecLam) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Công nghệ thông tin') RETURN v.noi_dung LIMIT 10

User: "Học phần tiên quyết của môn Giải tích 1?"
Cypher: MATCH (h1:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(h2:HocPhan) WHERE toLower(h2.ten) CONTAINS toLower('Giải tích 1') RETURN h1.ten as hoc_phan_tien_quyet LIMIT 5

User: "CTU có những chương trình đào tạo khóa 51 nào?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao {khoa: 51})-[:THUOC_VE]->(n:Nganh) RETURN n.ten_nganh_vi as ten_nganh, ctdt.loai_hinh as loai_hinh LIMIT 20

User: "Ngành Khoa học máy tính hệ chất lượng cao cần học tổng cộng bao nhiêu tín chỉ?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao {{loai_hinh: 'Chất lượng cao'}})-[:THUOC_VE]->(n:Nganh) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Khoa học máy tính') RETURN n.ten_nganh_vi, ctdt.loai_hinh, ctdt.tong_tin_chi LIMIT 1

User: "Môn Cơ sở dữ liệu có bao nhiêu tiết thực hành?"
Cypher: MATCH (h:HocPhan) WHERE toLower(h.ten) CONTAINS toLower('Cơ sở dữ liệu') RETURN h.ten, h.so_tiet_thuc_hanh LIMIT 5

User: "Liệt kê các môn học thuộc khối kiến thức giáo dục thể chất?"
Cypher: MATCH (k:KhoiKienThuc)-[:GOM_HOC_PHAN]->(h:HocPhan) WHERE toLower(k.ten) CONTAINS toLower('giáo dục thể chất') RETURN h.ten, h.so_tin_chi LIMIT 20

User: "Chuẩn đầu ra về kỹ năng của ngành Mạng máy tính là gì?"
Cypher: MATCH (n:Nganh)<-[:THUOC_VE]-(ctdt:ChuongTrinhDaoTao)-[:DAT_CHUAN_DAU_RA]->(cdr:ChuanDauRa) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Mạng máy tính') AND toLower(cdr.loai) CONTAINS 'kỹ năng' RETURN cdr.noi_dung LIMIT 10

User: "Ngành Kỹ thuật phần mềm thuộc khoa nào quản lý?"
Cypher: MATCH (khoa:Khoa)<-[:THUOC_VE]-(n:Nganh) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Kỹ thuật phần mềm') RETURN n.ten_nganh_vi, khoa.ten_khoa LIMIT 1

User: "Khóa 50 có những ngành đào tạo gì?"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao {khoa: 50})-[:THUOC_VE]->(n:Nganh) RETURN n.ten_nganh_vi, ctdt.loai_hinh LIMIT 15

User: "Tìm chương trình đào tạo khóa 52 ngành CNTT"
Cypher: MATCH (ctdt:ChuongTrinhDaoTao {khoa: 52})-[:THUOC_VE]->(n:Nganh) WHERE toLower(n.ten_nganh_vi) CONTAINS toLower('Công nghệ thông tin') RETURN n.ten_nganh_vi, ctdt.loai_hinh, ctdt.tong_tin_chi LIMIT 5
"""

# --- UTILITIES ---
_embed_model: Optional[EmbeddingModel] = None
_embed_model_lock = threading.Lock()
_embedding_cache: Dict[str, List[float]] = {}
_source_metrics = {"requests": 0, "graph": 0, "vector": 0, "none": 0, "start": time.time()}

def _log_metrics(source: str):
    _source_metrics["requests"] += 1
    _source_metrics[source] += 1
    print(f"[retrieval] Source: {source} | Total Req: {_source_metrics['requests']}")

def _get_embed_model() -> EmbeddingModel:
    global _embed_model
    if _embed_model is not None:
        return _embed_model

    with _embed_model_lock:
        if _embed_model is None:
            model_name = os.getenv("EMBEDDING_MODEL_NAME", "BAAI/bge-m3")
            cache_folder = os.getenv("EMBEDDING_CACHE_FOLDER", "../my_model_weights/bge_m3")
            _embed_model = EmbeddingModel(model_name=model_name, cache_folder=cache_folder)
    return _embed_model

def _is_safe_cypher(cypher: str) -> bool:
    if not cypher: return False
    forbidden = ["DELETE", "CREATE", "MERGE", "SET", "REMOVE", "DROP"]
    return not any(x in cypher.upper() for x in forbidden) and "RETURN" in cypher.upper()

def _humanize_column_name(col_name: str) -> str:
    """Convert Cypher column names to human-readable Vietnamese labels."""
    col_lower = col_name.lower()
    
    # Exact matches for common column names
    mappings = {
        "ten_nganh_vi": "Ngành",
        "ten_nganh_en": "Major (English)",
        "loai_hinh": "Loại hình",
        "ten": "Tên môn học",
        "so_tin_chi": "Tín chỉ",
        "so_tiet_ly_thuyet": "Tiết lý thuyết",
        "so_tiet_thuc_hanh": "Tiết thực hành",
        "noi_dung": "Nội dung",
        "ten_khoa": "Khoa/Bộ môn",
        "ten_bomon": "Bộ môn",
        "ma_trinh_do": "Cấp độ",
        "bat_buoc": "Bắt buộc/Tự chọn",
        "tong_tin_chi": "Tổng tín chỉ",
        "loai_van_bang": "Loại văn bằng",
        "ma_chuong_trinh": "Mã chương trình",
    }
    
    # Try exact match
    for key, val in mappings.items():
        if col_lower.endswith(key):
            return val
    
    # Strip prefixes and try again
    stripped = col_name.split(".")[-1] if "." in col_name else col_name
    for key, val in mappings.items():
        if stripped.lower() == key:
            return val
    
    # Fallback: improve readability of remaining column names
    # Remove common prefixes like "ctdt_", "h_", "n_"
    result = stripped
    for prefix in ["ctdt_", "h_", "n_", "v_", "k_", "cdr_"]:
        if result.startswith(prefix):
            result = result[len(prefix):]
            break
    
    # Convert snake_case to Title Case
    return " ".join([word.capitalize() for word in result.split("_")])

# --- CORE LOGIC ---

async def generate_cypher_with_llm(question: str) -> str:
    """Sử dụng 7B để biến ngôn ngữ tự nhiên thành Cypher dựa trên Schema chi tiết."""
    prompt = f"""
Bạn là chuyên gia Neo4j cho hệ thống đào tạo Đại học Cần Thơ.
Dựa trên Schema và các ví dụ sau, hãy viết câu lệnh Cypher để trả lời câu hỏi.

SCHEMA:
{GRAPH_SCHEMA}

VÍ DỤ:
{FEW_SHOT_EXAMPLES}

HƯỚNG DẪN QUAN TRỌNG:
1. Chỉ trả về duy nhất câu lệnh Cypher. KHÔNG GIẢI THÍCH GÌ.
2. LƯU Ý VỀ KIỂU DỮ LIỆU:
   - Thuộc tính "khoa" là SỐ NGUYÊN (VD: 51, 50, 52) → Không dùng ngoặc đơn. Ví dụ: {{khoa: 51}}
   - Thuộc tính "loai_hinh" là CHUỖI → Dùng ngoặc đơn. Ví dụ: {{loai_hinh: 'Chất lượng cao'}}
   - Tên thuộc tính để tìm kiếm → Dùng CONTAINS và toLower()
3. Luôn sử dụng CONTAINS và toLower() cho các thuộc tính tên để tìm kiếm linh hoạt.
4. Không thêm dấu ngoặc nhọn xung quanh câu trả lời.

Câu hỏi: {question}
Cypher:"""
    
    try:
        # Temperature 0.5 để model strictly tuân thủ ví dụ (cân bằng consistency vs flexibility)
        raw_cypher = await call_model_7b(prompt, temperature=0.5)
        # Làm sạch kết quả trả về từ LLM
        clean_cypher = raw_cypher.strip().replace("```cypher", "").replace("```", "").split(";")[0]
        return clean_cypher
    except Exception as e:
        logger.error(f"Lỗi sinh Cypher: {e}")
        return ""

def _execute_query(cypher: str) -> List[Dict]:
    try:
        driver = get_driver()
        with driver.session() as session:
            # Pass empty dict to prevent Neo4j driver from treating {} as format placeholders
            result = session.run(cypher, {})
            return result.data()
    except Exception as e:
        logger.error(f"Lỗi thực thi Cypher: {e}")
        return []

async def _get_vector_context(question: str) -> str:
    """Tìm kiếm vector dự phòng khi Graph không có kết quả."""
    model = _get_embed_model()
    # Chạy embedding trong thread để không block
    vec = await asyncio.to_thread(model.get_embedding_batch, [question])
    if not vec: return ""
    
    cypher_vector = """
    CALL db.index.vector.queryNodes('global_knowledge_index', 4, $vector)
    YIELD node, score WHERE score >= 0.65
    RETURN coalesce(node.text, node.noi_dung, node.ten_hp) as txt, score
    """
    rows = await asyncio.to_thread(_execute_query_with_params, cypher_vector, {"vector": vec[0]})
    return "\n".join([f"- {r['txt']}" for r in rows])

def _execute_query_with_params(cypher: str, params: dict):
    driver = get_driver()
    with driver.session() as session:
        return session.run(cypher, **params).data()

# --- MAIN EXPOSED FUNCTION ---

async def retrieve_graph_context(question: str) -> str:
    """
    Quy trình:
    1. Model 7B sinh Cypher dựa trên Schema chi tiết.
    2. Truy vấn Neo4j.
    3. Nếu không có data -> Fallback tìm kiếm Vector (BGE-M3).
    """
    # 1. Thử với Graph
    cypher = await generate_cypher_with_llm(question)
    graph_context = ""
    
    if _is_safe_cypher(cypher):
        logger.info(f"Generated Cypher: {cypher}")
        rows = await asyncio.to_thread(_execute_query, cypher)
        if rows:
            # Format lại dữ liệu để model dễ hiểu hơn
            formatted_rows = []
            for idx, row in enumerate(rows[:10], 1):
                row_items = []
                for k, v in row.items():
                    human_label = _humanize_column_name(k)
                    row_items.append(f"{human_label}: {v}")
                formatted_rows.append(f"{idx}. {' | '.join(row_items)}")
            
            # Extract filter context from Cypher to help model understand the query scope
            context_hint = ""
            
            # Check for khoa (batch/cohort) filter
            khoa_match = re.search(r'\{khoa:\s*(\d+)\}', cypher)
            if khoa_match:
                khoa_num = khoa_match.group(1)
                context_hint = f"[Khóa {khoa_num}] "
            
            # Check for loai_hinh (program type) filter
            loai_hinh_match = re.search(r"\{loai_hinh:\s*'([^']+)'\}", cypher)
            if loai_hinh_match:
                loai_hinh = loai_hinh_match.group(1)
                context_hint += f"[{loai_hinh}] "
            
            graph_context = f"DỮ LIỆU TỪ HỆ THỐNG (Từ Neo4j) {context_hint}:\n" + "\n".join(formatted_rows)

    # 2. Thử với Vector nếu Graph rỗng
    vector_context = ""
    if not graph_context:
        vector_context = await _get_vector_context(question)
        if vector_context:
            vector_context = "DỮ LIỆU TỪ THAM KHẢO (Từ Vector Search):\n" + vector_context

    # 3. Kết luận nguồn dữ liệu
    source = "graph" if graph_context else ("vector" if vector_context else "none")
    _log_metrics(source)

    # Kết hợp context từ cả hai nguồn với dấu ngăn cách rõ ràng
    final_context = ""
    if graph_context:
        final_context += graph_context
    if vector_context:
        if final_context:
            final_context += "\n\n---\n\n"
        final_context += vector_context
    
    return final_context.strip()

def warmup_embedding_model() -> None:
    """Khởi động model embedding để tránh độ trễ (cold-start) ở request đầu tiên."""
    logger.info("[retrieval] Bắt đầu warmup embedding model...")
    try:
        model = _get_embed_model()
        # Chạy thử một truy vấn mồi để ép PyTorch/Transformers load weights vào bộ nhớ
        vectors = model.get_embedding_batch(["warmup_query_ctu"])
        if vectors:
            _embedding_cache["warmup_query_ctu"] = vectors[0]
        logger.info("[retrieval] Warmup embedding model thành công!")
    except Exception as e:
        logger.error(f"[retrieval] Lỗi khi warmup embedding model: {e}")
