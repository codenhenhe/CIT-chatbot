import os
import json
import re
import requests
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import ollama
import asyncio

# Import các class đã xây dựng
from app.scripts.Embedding import EmbeddingModel
from app.scripts.neo4j_class import Neo4jConnector

# Nạp cấu hình từ .env
load_dotenv("../.env")

class GraphRAGChatbot:
    def __init__(self):
        print("--- Đang khởi tạo Embedding Model (BGE-M3)... ---")
        self.embed_service = EmbeddingModel(
            model_name="BAAI/bge-m3",
            device="cpu"
        )
        
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USERNAME", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        
        print("--- Đang kết nối tới Neo4j... ---")
        self.db = Neo4jConnector(uri, user, password)
        
        self.ollama_url = "http://localhost:11434/api/generate"
        self.llm_model = "qwen2.5-coder:3b-instruct"
        self.client = ollama.AsyncClient()

    def extract_intent_entity(self, query: str):
        query_lower = query.lower()

        # intent
        if "tiên quyết" in query_lower:
            intent = "prerequisite"
        elif "học phí" in query_lower:
            intent = "tuition"
        elif "tín chỉ" in query_lower:
            intent = "credit"
        else:
            intent = "general"

        # entity (đơn giản trước)
        entity = None
        match = re.search(r"(ct\d+)", query_lower)
        if match:
            entity = match.group()

        # hoặc tên môn
        if not entity:
            entity = query

        return intent, entity
        
    async def _call_llm(self, prompt: str, system_prompt: str = "") -> str:
        """Sử dụng AsyncClient để gọi Ollama không chặn luồng"""
        try:
            response = await self.client.generate(
                model=self.llm_model,
                system=system_prompt,
                prompt=prompt,
                options={"temperature": 0.0}
            )
            return response['response'].strip()
        except Exception as e:
            return f"Lỗi gọi LLM: {str(e)}"

    # --- TẦNG 1: TRUY VẤN SEMANTIC (Tìm kiếm Vector toàn cục) ---   
    # async def layer_1_semantic_search(self, query: str):
    #     query_vec = self.embed_service.get_embedding_batch([query])[0]
    #     cypher = """
    #     CALL db.index.vector.queryNodes('global_knowledge_index', 3, $vector)
    #     YIELD node, score
    #     WHERE score > 0.7
    #     RETURN node.text as context, labels(node)[0] as type
    #     ORDER BY score DESC LIMIT 2
    #     """
        
    #     try:
    #         # Sử dụng async session từ connector mới
    #         async with self.db.driver.session() as session:
    #             result = await session.run(cypher, vector=query_vec)
    #             records = await result.data()
    #             results = [f"[{r['type']}]: {r['context']}" for r in records]
    #             return "\n".join(results) if results else None
    #     except Exception as e:
    #         print(f"Lỗi Tầng 1: {e}")
    #     return None

    # TẦNG 2: HYBRID RETRIEVAL 
    # async def layer_2_hybrid_retrieval(self, query: str) -> Optional[str]:
    #     system_prompt = (
    #         "Bạn là trợ lý trích xuất thực thể từ câu hỏi. "
    #         "Nhiệm vụ: Trích xuất tên môn học hoặc mã môn học (Ví dụ: 'An ninh mạng', 'CT101'). "
    #         "Nếu hỏi về điều kiện học hoặc môn tiên quyết, đặt intent là 'tien_quyet'. "
    #         "Trả về duy nhất định dạng JSON: {'entity': '...', 'intent': '...'}. "
    #         "Nếu không thấy thực thể, trả về {'entity': null}."
    #     )
    #     response = await self._call_llm(query, system_prompt)
        
    #     try:
    #         match = re.search(r'\{.*\}', response, re.DOTALL)
    #         if not match: return None
    #         data = json.loads(match.group())
    #         entity = data.get("entity")
            
    #         if entity and data.get("intent") == "tien_quyet":
    #             # Tìm kiếm mờ (Fuzzy Search) bằng CONTAINS và toLower
    #             cypher = """
    #             MATCH (h:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(pre)
    #             WHERE toLower(h.ten_hp) CONTAINS toLower($id) OR h.ma_hp = $id
    #             RETURN DISTINCT h.ten_hp as target, collect(pre.ten_hp) as pres
    #             """
    #             async with self.db.driver.session() as session:
    #                 result = await session.run(cypher, id=entity)
    #                 res = await result.single()
    #                 if res:
    #                     return f"Dữ liệu đồ thị xác nhận: Môn {res['target']} yêu cầu phải học xong các môn tiên quyết sau: {', '.join(res['pres'])}."
    #     except:
    #         pass
    #     return None

    async def vector_search(self, query: str):
        query_vec = self.embed_service.get_embedding_batch([query])[0]

        cypher = """
        CALL db.index.vector.queryNodes('global_knowledge_index', 3, $vector)
        YIELD node, score
        WHERE score > 0.8
        RETURN node.text as context
        ORDER BY score DESC LIMIT 2
        """

        async with self.db.driver.session() as session:
            result = await session.run(cypher, vector=query_vec)
            records = await result.data()

        return "\n".join([r["context"] for r in records]) if records else None

    async def query_graph(self, intent: str, entity: str):
        try:
            async with self.db.driver.session() as session:

                if intent == "PREREQUISITE":
                    cypher = """
                    MATCH (h:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(pre)
                    WHERE toLower(h.ten_hp) CONTAINS toLower($name)
                    RETURN h.ten_hp as course, collect(pre.ten_hp) as prerequisites
                    """
                    result = await session.run(cypher, name=entity)
                    data = await result.single()
                    if data:
                        return f"Môn {data['course']} cần học trước: {', '.join(data['prerequisites'])}"

                elif intent == "CREDIT":
                    cypher = """
                    MATCH (h:HocPhan)
                    WHERE toLower(h.ten_hp) CONTAINS toLower($name)
                    RETURN h.ten_hp as course, h.so_tin_chi as credits
                    """
                    result = await session.run(cypher, name=entity)
                    data = await result.single()
                    if data:
                        return f"Môn {data['course']} có {data['credits']} tín chỉ"

        except Exception as e:
            print(f"Lỗi Graph: {e}")

        return None
    
    def build_context(self, graph_data, vector_data):
        context = ""

        if graph_data:
            context += f"[GRAPH]\n{graph_data}\n"

        if vector_data:
            context += f"[VECTOR]\n{vector_data}\n"

        return context.strip()

    # TẦNG 3: DYNAMIC CYPHER
    async def layer_3_graph_reasoning(self, query: str) -> str:
        schema = (
            "Nodes:\n"
            "- ChuongTrinhDaoTao(id, ma_chuong_trinh, khoa_hoc, loai_hinh, ngon_ngu, tong_tin_chi, thoi_gian): Thực thể trung tâm quản lý chương trình đào tạo.\n"
            "- HocPhan(id, ma_hp, ten_hp, so_tin_chi, so_tiet_ly_thuyet, so_tiet_thuc_hanh, bat_buoc): Chi tiết về môn học.\n"
            "- KhoiKienThuc(id, ten_khoi, tong_tin_chi, tin_chi_bat_buoc, tin_chi_tu_chon): Các khối như GD đại cương, Cơ sở ngành.\n"
            "- YeuCauTuChon(id, noi_dung_yeu_cau, mo_ta): Ràng buộc số tín chỉ tự chọn (VD: Chọn 10 TC).\n"
            "- NhomHocPhanTuChon(id, ten_nhom): Nhóm nhỏ môn học trong yêu cầu tự chọn.\n"
            "- ChuanDauRa(id, noi_dung, nhom, loai): Các chuẩn PLO/CDR phân theo Kiến thức/Kỹ năng/Thái độ.\n"
            "- VanBanPhapLy(id, so_hieu, ten_vb, loai_vb, ngay_ban_hanh, co_quan, noi_dung_goc): Quyết định ban hành hoặc văn bản căn cứ.\n"
            "- Nganh(id, ten_vi, ten_en): Thông tin ngành đào tạo.\n"
            "- Khoa(id, ten) & BoMon(id, ten): Đơn vị quản lý chuyên môn.\n"
            "- TrinhDo(id, ten): Cấp bậc đào tạo (Đại học/Cao đẳng).\n"
            "- LoaiVanBang(id, ten): Danh hiệu văn bằng (Kỹ sư/Cử nhân).\n"
            "- HinhThucDaoTao(id, ten) & PhuongThucDaoTao(id, ten): Cách thức tổ chức đào tạo.\n"
            "- MucTieuDaoTao(id, loai, noi_dung): Mục tiêu PEOs chung hoặc cụ thể.\n"
            "- ViTriViecLam(id, loai, noi_dung): Cơ hội nghề nghiệp sau tốt nghiệp.\n"
            "- ChuanThamKhao(id, noi_dung, link, noi_dung_goc): Tài liệu hoặc chuẩn tham chiếu quốc tế.\n"
            "- KhaNangHocTap(id, noi_dung): Định hướng học tập nâng cao.\n\n"

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
            "- (:YeuCauTuChon)-[:CO_NHOM_THAN_PHAN]->(:NhomHocPhanTuChon)\n"
            "- (:KhoiKienThuc|:YeuCauTuChon|:NhomHocPhanTuChon)-[:GOM_HOC_PHAN]->(:HocPhan)\n"
            "- (:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(:HocPhan)\n"
            "- (:HocPhan)-[:CO_THE_SONG_HANH]->(:HocPhan)\n"
        )

        system_prompt = (
            f"Bạn là một chuyên gia về cơ sở dữ liệu đồ thị Neo4j. Dựa trên schema sau đây:\n"
            f"{schema}\n\n"
            
            "### QUY TẮC TRUY VẤN:\n"
            "1. Ưu tiên dùng 'CONTAINS' và hàm 'toLower()' khi lọc tên để tránh lỗi chữ hoa/thường.\n"
            "2. Khi truy vấn về học phần, hãy nhớ 'HocPhan' có thể nối từ 'KhoiKienThuc' hoặc 'NhomHocPhanTuChon' qua quan hệ 'GOM_HOC_PHAN'.\n"
            "3. Chỉ trả về mã code Cypher trong khối ```cypher, không giải thích gì thêm.\n\n"

            "### VÍ DỤ FEW-SHOT:\n"
            
            "# Ví dụ 1: Tìm môn tiên quyết\n"
            "User: 'Môn nào là tiên quyết của môn Cấu trúc dữ liệu?'\n"
            "Assistant:\n"
            "```cypher\n"
            "MATCH (target:HocPhan)-[:YEU_CAU_TIEN_QUYET]->(pre:HocPhan)\n"
            "WHERE toLower(target.ten_hp) CONTAINS toLower('Cấu trúc dữ liệu')\n"
            "RETURN pre.ma_hp, pre.ten_hp\n"
            "```\n\n"

            "# Ví dụ 2: Tìm thông tin chương trình đào tạo của một ngành\n"
            "User: 'Chương trình đào tạo ngành An toàn thông tin khóa 50 có bao nhiêu tín chỉ?'\n"
            "Assistant:\n"
            "```cypher\n"
            "MATCH (ctdt:ChuongTrinhDaoTao)-[:THUOC_VE]->(n:Nganh)\n"
            "WHERE n.ten_vi CONTAINS 'An toàn thông tin' AND ctdt.khoa_hoc = 50\n"
            "RETURN ctdt.tong_tin_chi\n"
            "```\n\n"

            "# Ví dụ 3: Truy vấn phức tạp về nhóm tự chọn (dựa trên logic ETL của bạn)\n"
            "User: 'Trong khối chuyên ngành, tôi phải chọn bao nhiêu tín chỉ tự chọn?'\n"
            "Assistant:\n"
            "```cypher\n"
            "MATCH (k:KhoiKienThuc)-[:CO_YEU_CAU_TU_CHON]->(y:YeuCauTuChon)\n"
            "WHERE toLower(k.ten_khoi) CONTAINS toLower('chuyên ngành')\n"
            "RETURN y.noi_dung_yeu_cau, y.mo_ta\n"
            "```\n\n"

            "### CÂU HỎI CỦA NGƯỜI DÙNG:\n"
        )
        
        llm_response = await self._call_llm(query, system_prompt)
        cypher_match = re.search(r'```cypher(.*?)```', llm_response, re.DOTALL)
        
        if cypher_match:
            cypher_code = cypher_match.group(1).strip()
            print(f"-> Thực thi Cypher: {cypher_code}")
            try:
                async with self.db.driver.session() as session:
                    result = await session.run(cypher_code)
                    data = await result.data()
                    if data:
                        return f"Kết quả truy vấn đồ thị: {json.dumps(data, ensure_ascii=False)}"
            except Exception as e:
                return f"Lỗi truy vấn đồ thị: {e}"
        return "Hiện chưa tìm thấy thông tin chi tiết trong đồ thị đào tạo."

    # TỔNG HỢP CÂU TRẢ LỜI
    # async def final_synthesis(self, query: str, context: str) -> str:
    #     system_prompt = (
    #         "Bạn là chuyên viên tư vấn đào tạo của Đại học Cần Thơ. "
    #         "Dựa trên ngữ cảnh cung cấp từ đồ thị, hãy trả lời sinh viên một cách thân thiện. "
    #         "Ưu tiên liệt kê thông tin từ đồ thị. Nếu dữ liệu rỗng, hãy khuyên sinh viên liên hệ văn phòng Khoa."
    #     )
    #     return await self._call_llm(f"Context: {context}\n\nCâu hỏi: {query}", system_prompt)

    # async def ask(self, query: str):
    #     print(f"\n[Hỏi]: {query}")
        
    #     # Thứ tự ưu tiên: Tầng 2 (Chính xác nhất cho môn học) -> Tầng 1 (Thông tin rộng) -> Tầng 3 (Logic phức tạp)
        
    #     context = await self.layer_2_hybrid_retrieval(query)
    #     if context:
    #         print("-> Tầng 2: Đã trích xuất thực thể và dùng Template thành công.")
    #         return await self.final_synthesis(query, context)
            
    #     context = await self.layer_1_semantic_search(query)
    #     if context:
    #         print("-> Tầng 1: Tìm thấy ngữ cảnh tương đồng từ Vector Search.")
    #         return await self.final_synthesis(query, context)
            
    #     print("-> Tầng 3: Đang thực hiện suy luận logic trên đồ thị...")
    #     context = await self.layer_3_graph_reasoning(query)
    #     return await self.final_synthesis(query, context)

    def detect_intent(self, query: str):
        q = query.lower()

        # 1. greeting
        if any(x in q for x in ["xin chào", "hello", "hi", "chào", "hey"]):
            return "GREETING"

        # 2. cảm ơn
        if any(x in q for x in ["cảm ơn", "thanks", "thank"]):
            return "THANKS"

        # 3. học phí
        if "học phí" in q:
            return "TUITION"

        # 4. tín chỉ
        if "tín chỉ" in q:
            return "CREDIT"

        # 5. môn tiên quyết
        if any(x in q for x in ["tiên quyết", "học trước"]):
            return "PREREQUISITE"

        # 6. chương trình đào tạo
        if any(x in q for x in ["chương trình", "ctdt", "khung chương trình"]):
            return "PROGRAM"

        # 7. mô tả ngành
        if any(x in q for x in ["ngành", "học gì", "mô tả", "giới thiệu"]):
            return "MAJOR_INFO"

        # 8. cơ hội việc làm
        if any(x in q for x in ["việc làm", "nghề", "ra trường", "làm gì"]):
            return "CAREER"

        # 9. chuẩn đầu ra
        if any(x in q for x in ["chuẩn đầu ra", "plo"]):
            return "OUTCOME"

        # 10. hình thức đào tạo
        if any(x in q for x in ["chính quy", "tại chức", "online"]):
            return "TRAINING_FORM"

        # 11. thời gian học
        if any(x in q for x in ["bao lâu", "mấy năm", "thời gian học"]):
            return "DURATION"

        # 12. khoa / bộ môn
        if any(x in q for x in ["khoa", "bộ môn"]):
            return "FACULTY"

        # 13. văn bằng
        if any(x in q for x in ["bằng gì", "văn bằng"]):
            return "DEGREE"

        # 14. học tiếp
        if any(x in q for x in ["học tiếp", "cao học", "thạc sĩ"]):
            return "FURTHER_STUDY"

        # 15. fallback
        return "GENERAL"

    async def get_context(self, query: str):
        query = query.strip()

        print(f"\n[Hỏi]: {query}")

        if len(query) < 5:
            return None
        
        intent = self.detect_intent(query)

        # nếu không phải query học thuật → bỏ RAG
        if intent in ["GREETING", "THANKS"]:
            return None

        # 1. intent + entity
        _, entity = self.extract_intent_entity(query)

        # 2. query graph (ưu tiên)
        graph_data = await self.query_graph(intent, entity)

        # 3. nếu graph fail → vector
        vector_data = None
        if not graph_data:
            vector_data = await self.vector_search(query)

        # 4. build context
        context = self.build_context(graph_data, vector_data)

        return context if context else None

        # 5. gọi LLM
        # return await self.final_synthesis(query, context)

if __name__ == "__main__":
    bot = GraphRAGChatbot()
    try:
        async def main():
            context = await bot.get_context("Môn An ninh mạng cần học môn nào trước?")
            print(f"[BOT]: {context}")
        
        asyncio.run(main())
    finally:
        asyncio.run(bot.db.close())