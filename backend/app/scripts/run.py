import asyncio
from pathlib import Path
from typing import TYPE_CHECKING
from app.scripts.pdf_processing import PDFProcessor
from app.scripts.build_graph import QuyCheDocument, Neo4jGraphBuilder

if TYPE_CHECKING:
    from app.scripts.neo4j_class import Neo4jConnector


async def process_quy_che_hoc_vu(pdf_path: str, db_connector: "Neo4jConnector") -> dict:
    """
    Async pipeline để xử lý Quy chế học vụ:
    1. PDF → JSON (PDFProcessor)
    2. JSON → Neo4j Graph (Neo4jGraphBuilder)
    
    Args:
        pdf_path: Đường dẫn tới file PDF
        db_connector: Neo4jConnector instance để kết nối DB
        
    Returns:
        dict với kết quả ingestion (nodes, edges, metadata)
    """
    try:
        # 1. PDF → JSON
        processor = PDFProcessor(pdf_path)
        json_path = await asyncio.to_thread(processor.process)
        
        # 2. JSON → Graph (async)
        doc = QuyCheDocument(json_path)
        builder = Neo4jGraphBuilder(db_connector)
        await builder.build(doc)
        
        # Tính số lượng nodes và edges từ JSON (rough estimate)
        node_count = _count_nodes_from_doc(doc)
        edge_count = _count_edges_from_doc(doc)
        section_count = _count_sections_from_doc(doc)
        full_text = _collect_text_from_doc(doc)
        extracted_preview = full_text[:12000]
        extracted_text_length = len(full_text)
        
        return {
            "message": "Ingestion Neo4j thanh cong",
            "ingestion_applied": True,
            "nodes": node_count,
            "edges": edge_count,
            "extraction_source": "pdf_processing_fitz",
            "extracted_text_length": extracted_text_length,
            "section_count": section_count,
            "extracted_preview": extracted_preview,
        }
        
    except Exception as e:
        return {
            "message": f"Loi khi xu ly Quy che hoc vu: {str(e)}",
            "ingestion_applied": False,
            "error": str(e),
        }


def _count_nodes_from_doc(doc: QuyCheDocument) -> int:
    """Đếm số nodes từ QuyCheDocument"""
    count = 1  # VanBan node
    for chuong in doc.raw.get("quy_dinh_chi_tiet", []):
        count += 1  # Chuong node
        for dieu in chuong.get("cac_dieu", []):
            count += 1  # Dieu node
            for khoan in dieu.get("cac_khoan", []):
                count += 1  # Khoan node
                for _ in khoan.get("cac_diem", []):
                    count += 1  # Diem node
    return count


def _count_edges_from_doc(doc: QuyCheDocument) -> int:
    """Đếm số edges từ QuyCheDocument"""
    count = 0
    for chuong in doc.raw.get("quy_dinh_chi_tiet", []):
        count += 1  # VanBan -> Chuong
        for dieu in chuong.get("cac_dieu", []):
            count += 1  # Chuong -> Dieu
            for khoan in dieu.get("cac_khoan", []):
                count += 1  # Dieu -> Khoan
                count += len(khoan.get("cac_diem", []))  # Khoan -> Diem
    return count


def _count_sections_from_doc(doc: QuyCheDocument) -> int:
    """Đếm số section chính (Điều) để hiển thị metadata."""
    total = 0
    total += len(doc.raw.get("quyet_dinh_ban_hanh", {}).get("cac_dieu", []))
    for chuong in doc.raw.get("quy_dinh_chi_tiet", []):
        total += len(chuong.get("cac_dieu", []))
    return total


def _collect_text_from_doc(doc: QuyCheDocument) -> str:
    """Ghép nội dung đã parse thành text để thống kê và tạo preview."""
    parts: list[str] = []

    for dieu in doc.raw.get("quyet_dinh_ban_hanh", {}).get("cac_dieu", []):
        parts.append(dieu.get("id", ""))
        parts.append(dieu.get("tieu_de", ""))
        parts.append(dieu.get("noi_dung", ""))
        for khoan in dieu.get("cac_khoan", []):
            parts.append(khoan.get("noi_dung", ""))
            for diem in khoan.get("cac_diem", []):
                parts.append(diem.get("noi_dung", ""))

    for chuong in doc.raw.get("quy_dinh_chi_tiet", []):
        parts.append(chuong.get("chuong", ""))
        parts.append(chuong.get("ten", ""))
        for dieu in chuong.get("cac_dieu", []):
            parts.append(dieu.get("id", ""))
            parts.append(dieu.get("tieu_de", ""))
            parts.append(dieu.get("noi_dung", ""))
            for khoan in dieu.get("cac_khoan", []):
                parts.append(khoan.get("noi_dung", ""))
                for diem in khoan.get("cac_diem", []):
                    parts.append(diem.get("noi_dung", ""))

    return " ".join(p.strip() for p in parts if isinstance(p, str) and p.strip())


# if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--pdf", required=True)
    #
    # args = parser.parse_args()
    #
    # run_pipeline(args.pdf)