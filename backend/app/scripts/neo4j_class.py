import os
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
import logging
# from app.scripts.XuLyChuongTrinhDaoTao import CurriculumETL
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jConnector:
    def __init__(self, uri, user, password):
        try:
            self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
            # self.driver.verify_connectivity()
            logger.info("Kết nối Neo4j thành công!")
        except Exception as e:
            logger.error(f"Lỗi kết nối Neo4j: {e}")
            raise e

    async def close(self):
        if self.driver:
            await self.driver.close()
            logger.info("Đã đóng kết nối Neo4j.")

    async def create_constraints(self, labels):
        async with self.driver.session() as session: # Dùng async with
            for label in labels:
                query = f"CREATE CONSTRAINT unique_{label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE"
                await session.run(query)
                logger.info(f"Đã tạo constraint cho Label: {label}")

    async def add_nodes(self, nodes_data):
        query_template = """
        MERGE (n:{label} {{id: $id}})
        SET n:Entity
        SET n += $props
        """
        
        async with self.driver.session() as session:
            count = 0
            for node in nodes_data:
                if hasattr(node, "metadata"): 
                    # 1. Lấy metadata làm gốc
                    props = node.metadata.copy() 
                    label = props.get("type", "Entity")
                    node_id = node.node_id 
                    
                    # 2. CHỦ ĐỘNG THÊM TEXT VÀ EMBEDDING VÀO PROPS
                    props["text"] = node.text # Đưa nội dung văn bản vào node
                    
                    # Chỉ thêm embedding nếu nó tồn tại (đã chạy hàm generate_embeddings)
                    if hasattr(node, "embedding") and node.embedding is not None:
                        props["embedding"] = list(node.embedding) 
                    
                else:
                    # Xử lý Dictionary thường (giữ nguyên logic cũ của bạn)
                    label = node.get("type", "Entity")
                    node_id = node.get("id")
                    props = node.get("properties", {})

                if not node_id: continue

                try:
                    query = query_template.format(label=label)
                    await session.run(query, id=node_id, props=props)
                    count += 1
                except Exception as e:
                    logger.error(f"Lỗi khi thêm node {node_id}: {e}")
            
            logger.info(f"Đã nạp thành công {count} nodes.")

    async def add_edges(self, edges_data):
        """
        Thêm cạnh (Edges)
        """
        query_template = """
        MATCH (a {{id: $source_id}})
        MATCH (b {{id: $target_id}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += $props
        """

        async with self.driver.session() as session:
            count = 0
            for edge in edges_data:
                rel_type = edge.get("type", "RELATED_TO")
                source = edge.get("source")
                target = edge.get("target")
                props = edge.get("properties", {})

                if not source or not target: continue

                # Chỉ thay thế {rel_type}, còn {{id...}} sẽ trở thành {id...} chuẩn Cypher
                query = query_template.format(rel_type=rel_type)

                try:
                    await session.run(query, source_id=source, target_id=target, props=props)
                    count += 1
                except Exception as e:
                    logger.error(f"Lỗi nối cạnh {source}->{target}: {e}")
            
            logger.info(f"Đã nạp thành công {count} edges.")

    async def clear_database(self):
        """Hàm nguy hiểm: Xóa sạch dữ liệu (Dùng để test lại từ đầu)"""
        async with self.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")
            logger.warning("Đã xóa sạch database!")

    async def create_vector_index(self, index_name="global_knowledge_index", label="Entity", property="embedding", dimensions=1024, similarity="cosine"):
        """
        Tạo Vector Index để hỗ trợ tìm kiếm ngữ nghĩa (Tầng 1 Chatbot)
        """
        query = f"""
        CREATE VECTOR INDEX {index_name} IF NOT EXISTS
        FOR (n:{label})
        ON (n.{property})
        OPTIONS {{indexConfig: {{
         `vector.dimensions`: {dimensions},
         `vector.similarity_function`: '{similarity}'
        }}}}
        """
        async with self.driver.session() as session:
            try:
                await session.run(query)
                logger.info(f"Đã khởi tạo Vector Index: {index_name} trên Label: {label}")
            except Exception as e:
                logger.error(f"Lỗi khi tạo Vector Index: {e}")

# Cấu hình kết nối
# URI = "bolt://localhost:7687"
# USER = "neo4j"
# PASSWORD = "admin1234"
# load_dotenv(".env")

# neo4j_username = os.getenv("NEO4J_USERNAME")
# neo4j_password = os.getenv("NEO4J_PASSWORD")
# neo4j_uri = os.getenv("NEO4J_URI")
# PDF_FILE = "data/pdf/ChuyenNganh_DaoTao/pdf/66_7480201_CongNgheThongTin.signed.signed.signed.signed.pdf"
# PDF_FILE = "data/pdf/ChuyenNganh_DaoTao/pdf/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
# # PDF_FILE = "data/pdf/ChuyenNganh_DaoTao/pdf/73_7480101_KhoaHocMayTinh.signed.signed.signed.signed.signed.pdf"


# db = None
# try:
#     # Khởi tạo
#     db = Neo4jConnector(neo4j_uri, neo4j_username, neo4j_password)

#     # 2. Tạo Constraint cho ID để tìm kiếm nhanh và không bị trùng
#     # Liệt kê các Label trong Class CurriculumETL
#     labels_to_index = ["ChuongTrinhDaoTao", "HocPhan", "HinhThucDaoTao", "LoaiVanBang", 
#                        "PhuongThucDaoTao", "TrinhDo", "Khoa", "BoMon", "Nganh", "VanBanPhapLy", 
#                        "ViTriViecLam", "MucTieuDaoTao", "ChuanDauRa", "KhaNangHocTap", "KhoiKienThuc", 
#                        "ChuanThamKhao", "NhomHocPhanTuChon", "YeuCauTuChon"]
#     db.create_constraints(labels_to_index)

#     etl = CurriculumETL(PDF_FILE)
#     etl.process()

#     # nạp dữ liệu
#     # db.clear_database() 
#     db.add_nodes(etl.nodes)
#     db.add_edges(etl.edges)
#     db.create_vector_index()

# except Exception as e:
#     print(f"Có lỗi xảy ra: {e}")
# finally:
#     if db:
#         db.close()