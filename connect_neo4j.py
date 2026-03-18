from neo4j import GraphDatabase

# 1. Cấu hình thông tin kết nối
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "admin1234")  

def ket_noi_va_lay_du_lieu():
    # 2. Tạo driver kết nối
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    print("Đang kết nối đến Neo4j...")
    
    try:
        # driver.session(database="neo4j") # Chỉ định database nếu cần
        # 3. Tạo phiên làm việc (Session)
        with driver.session() as session:
            # Viết câu lệnh Cypher ở đây
            # cau_lenh = "MATCH (n) RETURN n LIMIT 5"
            cau_lenh = """
            MATCH (s)-[r]->(e) 
            RETURN s, r, e 
            LIMIT 5
            """
            
            # Chạy lệnh
            ket_qua = session.run(cau_lenh)
            
            # 4. Xử lý kết quả trả về
            print("Kết quả tìm thấy:")
            # for record in ket_qua:
            #     print(record)
            #     # Lấy toàn bộ node
            #     node = record["n"]
            #     print(f"Node: {set(node.labels)}")
            #     # In ra ID và thuộc tính (properties)
            #     print(f"- Node ID: {node.element_id}")
            #     print(f"  Dữ liệu: {node._properties}")
            #     print("-" * 20)
            for record in ket_qua:
                # 1. Lấy Node nguồn (Start Node)
                source = record["s"]
                ten_nguon = source.get("ten", "Không tên") # Dùng .get để tránh lỗi nếu không có tên
                
                # 2. Lấy Mối quan hệ (Relationship)
                rel = record["r"]
                loai_quan_he = rel.type  # Lấy tên loại quan hệ (ví dụ: DANG_HOC)
                thuoc_tinh_qh = rel._properties # Lấy các thuộc tính (ví dụ: muc_do)
                
                # 3. Lấy Node đích (End Node)
                target = record["e"]
                ten_dich = target.get("ten", "Không tên")
                
                # 4. In ra màn hình cho đẹp
                print(f"{ten_nguon} --[{loai_quan_he}]--> {ten_dich}")
                print(f"   + Chi tiết quan hệ: {thuoc_tinh_qh}")
                print("-" * 30)
                
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
    finally:
        # 5. Đóng kết nối
        driver.close()
        print("Đã đóng kết nối.")

if __name__ == "__main__":
    ket_noi_va_lay_du_lieu()