import argparse
import asyncio
import json
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

if __package__ in (None, ""):
	backend_root = Path(__file__).resolve().parents[2]
	if str(backend_root) not in sys.path:
		sys.path.append(str(backend_root))

KEY_FIELDS: Dict[str, List[str]] = {
	"VanBanPhapLy": ["so", "ten"],
	"Nganh": ["ma_nganh", "ten_nganh_vi", "ten_nganh_en"],
	"Khoa": ["ten_khoa"],
	"BoMon": ["ten_bo_mon"],
	"ChuongTrinhDaoTao": ["ma_chuong_trinh"],
	"LoaiVanBang": ["loai_van_bang"],
	"HinhThucDaoTao": ["ten_hinh_thuc"],
	"PhuongThucDaoTao": ["ten_phuong_thuc"],
	"TrinhDo": ["ma_trinh_do", "ten_trinh_do"],
	"DoiTuongTuyenSinh": ["noi_dung"],
	"DieuKienTotNghiep": ["noi_dung"],
	"MucTieuDaoTao": ["noi_dung"],
	"ViTriViecLam": ["noi_dung"],
	"ChuanThamKhao": ["noi_dung", "link", "noi_dung_goc"],
	"DanhGiaKiemDinh": ["noi_dung"],
	"KhaNangHocTap": ["noi_dung"],
	"ChuanDauRa": ["ma_chuan", "noi_dung"],
	"KhoiKienThuc": ["ma_khoi", "ten_khoi"],
	"YeuCauTuChon": ["noi_dung_yeu_cau"],
	"NhomHocPhanTuChon": ["ten_nhom"],
	"HocPhan": ["ma_hoc_phan", "ten_hoc_phan", "dieu_kien", "yeu_cau_stc_toi_thieu"],
}

ID_FIELDS: Dict[str, List[str]] = {
	"VanBanPhapLy": ["so"],
	"TrinhDo": ["ma_trinh_do"],
	"Nganh": ["ma_nganh"],
	"Khoa": ["ma_khoa"],
	"BoMon": ["ma_bo_mon"],
	"ChuongTrinhDaoTao": ["ma_chuong_trinh"],
	"LoaiVanBang": ["ma_loai"],
	"HinhThucDaoTao": ["ma_hinh_thuc"],
	"PhuongThucDaoTao": ["ma_phuong_thuc"],
	"ChuanDauRa": ["ma_chuan"],
	"KhoiKienThuc": ["ma_khoi"],
	"HocPhan": ["ma_hoc_phan"],
}

NODE_TYPE_ALIASES: Dict[str, str] = {
	"CoHoiViecLam": "ViTriViecLam",
	"QuyDinh": "VanBanPhapLy",
	"Faculty": "Khoa",
	"Department": "BoMon",
	"Major": "Nganh",
	"Program": "ChuongTrinhDaoTao",
}


def _clean_text(value: Any) -> str:
	if value is None:
		return ""
	return re.sub(r"\s+", " ", str(value)).strip()


def _normalize_token(value: Any) -> str:
	text = _clean_text(value).lower()
	text = unicodedata.normalize("NFD", text)
	text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
	text = text.replace("đ", "d")
	text = re.sub(r"[^a-z0-9\s]", " ", text)
	return re.sub(r"\s+", " ", text).strip()


def _slugify(value: str) -> str:
	text = unicodedata.normalize("NFD", _clean_text(value))
	text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
	text = text.replace("Đ", "D").replace("đ", "d")
	text = re.sub(r"[^A-Za-z0-9]+", "_", text)
	return text.strip("_").upper() or "UNKNOWN"


def _node_type_token(node_type: str) -> str:
	return _slugify(node_type)


def _default_json_path_from_pdf(pdf_path: str) -> str:
	backend_root = Path(__file__).resolve().parents[2]
	output_dir = backend_root / "processed_data" / "json"
	output_dir.mkdir(parents=True, exist_ok=True)
	return str(output_dir / f"{Path(pdf_path).stem}.json")


def _load_extracted_json(json_path: str) -> Dict[str, Any]:
	with open(json_path, "r", encoding="utf-8") as f:
		return json.load(f)


def _make_node_id(node_type: str, item: Dict[str, Any], index: int, program_hint: str) -> str:
	for field in ID_FIELDS.get(node_type, []):
		value = _clean_text(item.get(field))
		if value:
			return _slugify(value)

	program_token = _slugify(program_hint)
	return f"{program_token}_{_node_type_token(node_type)}_{index + 1}"


def _is_empty_item(item: Dict[str, Any]) -> bool:
	for value in item.values():
		if value is None:
			continue
		if isinstance(value, bool):
			return False
		if isinstance(value, (int, float)):
			return False
		if _clean_text(value):
			return False
	return True


def _build_text_for_node(node_type: str, item: Dict[str, Any]) -> str:
	existing_text = _clean_text(item.get("text"))
	if existing_text:
		return existing_text

	if node_type == "HocPhan":
		dieu_kien = _clean_text(item.get('dieu_kien')).lower()
		yeu_cau_stc = _clean_text(item.get('yeu_cau_stc_toi_thieu'))
		parts = [
			f"Hoc phan {_clean_text(item.get('ma_hoc_phan'))} - {_clean_text(item.get('ten_hoc_phan'))}.",
			f"So tin chi: {_clean_text(item.get('so_tin_chi'))}.",
			f"So tiet LT: {_clean_text(item.get('so_tiet_ly_thuyet'))}.",
			f"So tiet TH: {_clean_text(item.get('so_tiet_thuc_hanh'))}.",
		]
		if dieu_kien not in {'false', '0', ''}:
			parts.append("Co dieu kien hoc truoc.")
		if yeu_cau_stc:
			parts.append(f"Yeu cau STC toi thieu: {yeu_cau_stc}.")
		return " ".join(parts)
	if node_type == "KhoiKienThuc":
		return (
			f"Khoi kien thuc {_clean_text(item.get('ten_khoi'))} "
			f"(ma {_clean_text(item.get('ma_khoi'))}), tong tin chi {_clean_text(item.get('tong_tin_chi'))}."
		)
	if node_type == "ChuanThamKhao":
		base = _clean_text(item.get("noi_dung")) or _clean_text(item.get("noi_dung_goc"))
		link = _clean_text(item.get("link"))
		return f"Chuan tham khao: {base}. Link: {link}." if link else f"Chuan tham khao: {base}."

	details = [
		f"{k}: {_clean_text(v)}"
		for k, v in item.items()
		if k not in {"text", "embedding"} and _clean_text(v)
	]
	detail_text = "; ".join(details)
	return f"{node_type}: {detail_text}" if detail_text else node_type


def _build_nodes_from_results(results: Dict[str, Any], source_name: str) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
	nodes: List[Dict[str, Any]] = []
	index_map: Dict[str, List[Dict[str, Any]]] = {}

	ctdt_items = results.get("ChuongTrinhDaoTao", {}).get("items", [])
	program_hint = _clean_text(ctdt_items[0].get("ma_chuong_trinh")) if ctdt_items else Path(source_name).stem
	if not program_hint:
		program_hint = Path(source_name).stem

	fallback_counts: Dict[str, int] = {}
	for node_type, payload in results.items():
		items = payload.get("items", []) if isinstance(payload, dict) else []
		if not isinstance(items, list):
			continue
		count = 0
		for item in items:
			if not isinstance(item, dict):
				continue
			has_code = any(_clean_text(item.get(field)) for field in ID_FIELDS.get(node_type, []))
			if not has_code:
				count += 1
		fallback_counts[node_type] = count

	for node_type, payload in results.items():
		items = payload.get("items", []) if isinstance(payload, dict) else []
		if not isinstance(items, list):
			continue

		# Lam sach node Khoa: neu da co item chua tu "khoa" thi bo cac item truong/chung chung bi trich nham.
		if node_type == "Khoa":
			has_real_khoa = any("khoa" in _normalize_token((item or {}).get("ten_khoa")) for item in items if isinstance(item, dict))
			if has_real_khoa:
				items = [
					item
					for item in items
					if isinstance(item, dict) and "khoa" in _normalize_token(item.get("ten_khoa"))
				]

		fallback_index = 0
		for idx, item in enumerate(items):
			if not isinstance(item, dict):
				continue
			if _is_empty_item(item):
				continue
			has_code = any(_clean_text(item.get(field)) for field in ID_FIELDS.get(node_type, []))
			if has_code:
				node_id = _make_node_id(node_type, item, idx, program_hint)
			else:
				fallback_index += 1
				program_token = _slugify(program_hint)
				node_token = _node_type_token(node_type)
				if fallback_counts.get(node_type, 0) > 1:
					node_id = f"{program_token}_{node_token}_{fallback_index}"
				else:
					node_id = f"{program_token}_{node_token}"
			text = _build_text_for_node(node_type, item)
			properties = dict(item)
			properties["text"] = text

			node_obj = {
				"type": node_type,
				"id": node_id,
				"properties": properties,
			}
			nodes.append(node_obj)

			tokens = {node_id, _normalize_token(node_id), _normalize_token(text)}
			for field in KEY_FIELDS.get(node_type, []):
				val = _clean_text(item.get(field))
				if val:
					tokens.add(_normalize_token(val))

			index_map.setdefault(node_type, []).append(
				{
					"id": node_id,
					"tokens": tokens,
				}
			)

	return nodes, index_map


def _resolve_node_id(node_type: str, match_value: Any, index_map: Dict[str, List[Dict[str, Any]]]) -> Optional[str]:
	match_norm = _normalize_token(match_value)
	if not match_norm:
		return None

	normalized_type = NODE_TYPE_ALIASES.get(_clean_text(node_type), _clean_text(node_type))
	candidates = index_map.get(normalized_type, [])
	if not candidates:
		return None

	# ChuongTrinhDaoTao thuong chi co 1 node, cho phep fallback de khong mat rel.
	if normalized_type == "ChuongTrinhDaoTao" and len(candidates) == 1:
		return candidates[0]["id"]

	for candidate in candidates:
		if match_norm in candidate["tokens"]:
			return candidate["id"]

	for candidate in candidates:
		if any(match_norm in token or token in match_norm for token in candidate["tokens"] if token):
			return candidate["id"]
	return None


def _build_edges_from_relationships(
	relationships: List[Dict[str, Any]],
	index_map: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
	edges: List[Dict[str, Any]] = []
	seen = set()

	for rel in relationships:
		if not isinstance(rel, dict):
			continue

		src_type = _clean_text(rel.get("source_node_type"))
		tgt_type = _clean_text(rel.get("target_node_type"))
		rel_type = _clean_text(rel.get("rel_type")) or "RELATED_TO"

		source_id = _resolve_node_id(src_type, rel.get("source_match"), index_map)
		target_id = _resolve_node_id(tgt_type, rel.get("target_match"), index_map)
		if not source_id or not target_id:
			continue

		key = (source_id, rel_type, target_id)
		if key in seen:
			continue
		seen.add(key)

		edges.append(
			{
				"source": source_id,
				"target": target_id,
				"type": rel_type,
				"properties": {
					"evidence": _clean_text(rel.get("evidence")),
					"source_node_type": src_type,
					"target_node_type": tgt_type,
				},
			}
		)

	return edges


def _node_ids(index_map: Dict[str, List[Dict[str, Any]]], node_type: str) -> List[str]:
	normalized_type = NODE_TYPE_ALIASES.get(node_type, node_type)
	return [item["id"] for item in index_map.get(normalized_type, [])]


def _infer_core_edges(index_map: Dict[str, List[Dict[str, Any]]], existing_edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	"""Bo sung cac quan he cot loi theo ETL de tranh mat lien ket khi JSON relation thieu/lech type."""
	seen = {(edge["source"], edge["type"], edge["target"]) for edge in existing_edges}
	inferred: List[Dict[str, Any]] = []

	def _add_relation(source_ids: List[str], rel_type: str, target_ids: List[str]) -> None:
		for source_id in source_ids:
			for target_id in target_ids:
				key = (source_id, rel_type, target_id)
				if key in seen:
					continue
				seen.add(key)
				inferred.append(
					{
						"source": source_id,
						"target": target_id,
						"type": rel_type,
						"properties": {
							"evidence": "inferred_from_etl_schema",
							"source_node_type": "inferred",
							"target_node_type": "inferred",
						},
					}
				)

	program_ids = _node_ids(index_map, "ChuongTrinhDaoTao")
	major_ids = _node_ids(index_map, "Nganh")
	faculty_ids = _node_ids(index_map, "Khoa")
	dept_ids = _node_ids(index_map, "BoMon")

	# Theo ETL: CTDT -> Nganh, Nganh -> Khoa/BoMon.
	_add_relation(program_ids, "THUOC_VE", major_ids)
	_add_relation(major_ids, "THUOC_VE", faculty_ids + dept_ids)

	return inferred


def _attach_embeddings(nodes: List[Dict[str, Any]], embedder: Any) -> None:
	texts = [node.get("properties", {}).get("text", "") for node in nodes]
	embeddings = embedder.get_embedding_batch(texts)
	if len(embeddings) != len(nodes):
		raise RuntimeError("So luong embedding khong khop so luong node.")

	for idx, emb in enumerate(embeddings):
		nodes[idx].setdefault("properties", {})["embedding"] = list(emb)


async def import_json_to_neo4j(json_path: str) -> Dict[str, Any]:
	from app.scripts.Embedding import get_embedding_model
	from app.scripts.neo4j_class import Neo4jConnector

	data = _load_extracted_json(json_path)
	results = data.get("results", {}) if isinstance(data, dict) else {}
	relationships = data.get("relationships", []) if isinstance(data, dict) else []
	source_name = _clean_text(data.get("source")) or Path(json_path).stem

	nodes, index_map = _build_nodes_from_results(results, source_name)
	edges = _build_edges_from_relationships(relationships, index_map)
	edges.extend(_infer_core_edges(index_map, edges))

	embedder = get_embedding_model()  # Lấy global instance
	_attach_embeddings(nodes, embedder)

	load_dotenv(".env")
	neo4j_uri = os.getenv("NEO4J_URI")
	neo4j_username = os.getenv("NEO4J_USERNAME")
	neo4j_password = os.getenv("NEO4J_PASSWORD")

	if not neo4j_uri or not neo4j_username or not neo4j_password:
		raise RuntimeError("Thieu NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD trong .env")

	db = Neo4jConnector(neo4j_uri, neo4j_username, neo4j_password)
	try:
		labels = sorted({node["type"] for node in nodes})
		await db.create_constraints(labels)
		await db.add_nodes(nodes)
		await db.add_edges(edges)
		await db.create_vector_index(
			index_name="global_knowledge_index",
			label="Entity",
			property="embedding",
			dimensions=len(nodes[0]["properties"]["embedding"]) if nodes else 1024,
			similarity="cosine",
		)
	finally:
		await db.close()

	return {
		"json_path": json_path,
		"node_count": len(nodes),
		"edge_count": len(edges),
	}


async def run_full_pipeline(pdf_path: str, clear_db: bool = False) -> Dict[str, Any]:
	from app.scripts.curriculum_main import run_curriculum_etl
	from app.scripts.neo4j_class import Neo4jConnector

	json_path = _default_json_path_from_pdf(pdf_path)
	await run_curriculum_etl(pdf_path=pdf_path, output_json=json_path)

	if clear_db:
		load_dotenv(".env")
		neo4j_uri = os.getenv("NEO4J_URI")
		neo4j_username = os.getenv("NEO4J_USERNAME")
		neo4j_password = os.getenv("NEO4J_PASSWORD")
		if not neo4j_uri or not neo4j_username or not neo4j_password:
			raise RuntimeError("Thieu NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD trong .env")
		db = Neo4jConnector(neo4j_uri, neo4j_username, neo4j_password)
		try:
			await db.clear_database()
		finally:
			await db.close()

	import_summary = await import_json_to_neo4j(json_path=json_path)
	import_summary["pdf_path"] = pdf_path
	return import_summary


def dry_run_graph_build(json_path: str) -> Dict[str, Any]:
	data = _load_extracted_json(json_path)
	results = data.get("results", {}) if isinstance(data, dict) else {}
	relationships = data.get("relationships", []) if isinstance(data, dict) else []
	source_name = _clean_text(data.get("source")) or Path(json_path).stem
	nodes, index_map = _build_nodes_from_results(results, source_name)
	edges = _build_edges_from_relationships(relationships, index_map)
	return {
		"json_path": json_path,
		"node_count": len(nodes),
		"edge_count": len(edges),
		"labels": sorted({node.get("type") for node in nodes if node.get("type")}),
	}


def main(
	json_path: str = "",
	pdf_path: str = "",
	run_etl: bool = False,
	clear_db: bool = False,
	dry_run: bool = False,
) -> Dict[str, Any]:
	"""
	Main function to process curriculum data and import to Neo4j.
	
	Args:
		json_path: Path to extracted JSON file.
		pdf_path: PDF path to infer JSON path at processed_data/json/<pdf>.json
		run_etl: Run ETL from PDF first, then import graph to Neo4j in one file.
		clear_db: Clear Neo4j database before importing graph.
		dry_run: Only build node/edge payload from JSON, do not connect Neo4j.
	
	Returns:
		Dictionary containing summary of the operation.
	"""
	json_path = _clean_text(json_path)
	pdf_path = _clean_text(pdf_path)

	if run_etl:
		if not pdf_path:
			raise ValueError("pdf_path is required when run_etl is True")
		summary = asyncio.run(run_full_pipeline(pdf_path=pdf_path, clear_db=clear_db))
	else:
		if not json_path:
			if not pdf_path:
				raise ValueError("Either json_path or pdf_path must be provided")
			json_path = _default_json_path_from_pdf(pdf_path)

		if dry_run:
			summary = dry_run_graph_build(json_path)
		else:
			if clear_db:
				# Optional cleanup when user explicitly requests reimport from scratch.
				from app.scripts.neo4j_class import Neo4jConnector
				load_dotenv(".env")
				neo4j_uri = os.getenv("NEO4J_URI")
				neo4j_username = os.getenv("NEO4J_USERNAME")
				neo4j_password = os.getenv("NEO4J_PASSWORD")
				if not neo4j_uri or not neo4j_username or not neo4j_password:
					raise RuntimeError("Thieu NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD trong .env")
				db = Neo4jConnector(neo4j_uri, neo4j_username, neo4j_password)
				try:
					asyncio.run(db.clear_database())
				finally:
					asyncio.run(db.close())
			summary = asyncio.run(import_json_to_neo4j(json_path))
	
	return summary


def _parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Import curriculum extracted JSON to Neo4j.")
	parser.add_argument("--json", dest="json_path", default="", help="Path to extracted JSON file.")
	parser.add_argument("--pdf", dest="pdf_path", default="", help="PDF path to infer JSON path at processed_data/json/<pdf>.json")
	parser.add_argument("--run-etl", action="store_true", help="Run ETL from PDF first, then import graph to Neo4j in one file.")
	parser.add_argument("--clear-db", action="store_true", help="Clear Neo4j database before importing graph.")
	parser.add_argument("--dry-run", action="store_true", help="Only build node/edge payload from JSON, do not connect Neo4j.")
	return parser.parse_args()


# if __name__ == "__main__":
# 	# pp = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
# 	pp = "data/2024_MT_KHMT.pdf"
# 	summary = main(
# 		pdf_path=pp,
# 		run_etl=True,
# 		clear_db=False,
# 	)
# 	print(json.dumps(summary, ensure_ascii=False, indent=2))
