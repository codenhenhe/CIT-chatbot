import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


if __package__ in (None, ""):
	backend_root = Path(__file__).resolve().parents[2]
	if str(backend_root) not in sys.path:
		sys.path.append(str(backend_root))

from app.services.llm_service import call_model_7b


NODE_FIELD_SCHEMAS: Dict[str, List[str]] = {
	"VanBanPhapLy": ["so", "ten", "loai", "ngay_ban_hanh", "co_quan_ban_hanh", "noi_dung_goc"],
	"TrinhDo": ["ten_trinh_do"],
	"Nganh": ["ma_nganh", "ten_nganh_vi", "ten_nganh_en"],
	"Khoa": ["ten_khoa"],
	"BoMon": ["ten_bo_mon"],
	"ChuongTrinhDaoTao": ["ma_chuong_trinh", "khoa", "he", "ngon_ngu", "tong_tin_chi", "thoi_gian_dao_tao"],
	"LoaiVanBang": ["loai_van_bang"],
	"HinhThucDaoTao": ["ten_hinh_thuc"],
	"PhuongThucDaoTao": ["ten_phuong_thuc"],
	"MucTieuDaoTao": ["loai", "noi_dung"],
	"ViTriViecLam": ["noi_dung"],
	"ChuanThamKhao": ["noi_dung", "link", "noi_dung_goc"],
	"KhaNangHocTap": ["noi_dung"],
	"ChuanDauRa": ["ma_chuan", "nhom", "loai", "noi_dung"],
	"KhoiKienThuc": ["ma_khoi", "ten_khoi", "tong_tin_chi", "tin_chi_bat_buoc", "tin_chi_tu_chon"],
	"YeuCauTuChon": ["noi_dung_yeu_cau", "so_tin_chi_yeu_cau"],
	"NhomHocPhanTuChon": ["ten_nhom"],
	"HocPhan": ["ma_hp", "ten_hp", "so_tin_chi", "so_tiet_ly_thuyet", "so_tiet_thuc_hanh", "bat_buoc"],
}

RELATION_TYPES: List[str] = [
	"DUOC_BAN_HANH_THEO",
	"DAO_TAO_TRINH_DO",
	"THUOC_VE",
	"CO_LOAI_VAN_BANG",
	"DAO_TAO_THEO_HINH_THUC",
	"DAO_TAO_THEO_PHUONG_THUC",
	"CO_QUY_DINH",
	"CO_MUC_TIEU_DAO_TAO",
	"CO_CO_HOI_VIEC_LAM",
	"TUAN_THU",
	"THAM_CHIEU",
	"TAO_NEN_TANG",
	"DAT_CHUAN_DAU_RA",
	"CO_KHOI_KIEN_THUC",
	"CO_YEU_CAU_TU_CHON",
	"CO_NHOM_THANH_PHAN",
	"GOM_HOC_PHAN",
	"YEU_CAU_TIEN_QUYET",
	"CO_THE_SONG_HANH",
]


def _to_int(value: Any) -> Optional[int]:
	if value is None:
		return None
	try:
		return int(str(value).strip())
	except Exception:
		return None


def _to_float(value: Any) -> Optional[float]:
	if value is None:
		return None
	try:
		return float(str(value).replace(",", ".").strip())
	except Exception:
		return None


def _to_bool(value: Any) -> Optional[bool]:
	if isinstance(value, bool):
		return value
	if value is None:
		return None
	v = str(value).strip().lower()
	if v in ["true", "1", "yes", "co", "có", "bat buoc", "bắt buộc"]:
		return True
	if v in ["false", "0", "no", "khong", "không", "tu chon", "tự chọn"]:
		return False
	return None


def _clean_text(value: Any) -> Optional[str]:
	if value is None:
		return None
	v = str(value).strip()
	return v if v else None


def _postprocess_ChuongTrinhDaoTao(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	for item in items:
		item["ma_chuong_trinh"] = _clean_text(item.get("ma_chuong_trinh"))
		item["khoa"] = _to_int(item.get("khoa"))
		item["loai_hinh"] = _clean_text(item.get("loai_hinh"))
		item["ngon_ngu"] = _clean_text(item.get("ngon_ngu"))
		item["tong_tin_chi"] = _to_int(item.get("tong_tin_chi"))
		item["thoi_gian_dao_tao"] = _to_float(item.get("thoi_gian_dao_tao"))
	return items


def _postprocess_HocPhan(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	for item in items:
		item["ma_hp"] = _clean_text(item.get("ma_hp"))
		item["ten_hp"] = _clean_text(item.get("ten_hp"))
		item["so_tin_chi"] = _to_int(item.get("so_tin_chi"))
		item["so_tiet_ly_thuyet"] = _to_int(item.get("so_tiet_ly_thuyet"))
		item["so_tiet_thuc_hanh"] = _to_int(item.get("so_tiet_thuc_hanh"))
		item["bat_buoc"] = _to_bool(item.get("bat_buoc"))
	return items


def _postprocess_ChuanDauRa(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	for item in items:
		item["id"] = _clean_text(item.get("id"))
		item["nhom"] = _clean_text(item.get("nhom"))
		item["loai"] = _clean_text(item.get("loai"))
		item["noi_dung"] = _clean_text(item.get("noi_dung"))
	return items


def _postprocess_KhoiKienThuc(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	for item in items:
		item["ma_khoi"] = _clean_text(item.get("ma_khoi"))
		item["ten_khoi"] = _clean_text(item.get("ten_khoi"))
		item["tong_tin_chi"] = _to_int(item.get("tong_tin_chi"))
		item["tin_chi_bat_buoc"] = _to_int(item.get("tin_chi_bat_buoc"))
		item["tin_chi_tu_chon"] = _to_int(item.get("tin_chi_tu_chon"))
	return items


def _postprocess_default(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	for item in items:
		for k, v in item.items():
			if isinstance(v, str):
				item[k] = _clean_text(v)
	return items


NODE_POSTPROCESSORS = {
	"ChuongTrinhDaoTao": _postprocess_ChuongTrinhDaoTao,
	"HocPhan": _postprocess_HocPhan,
	"ChuanDauRa": _postprocess_ChuanDauRa,
	"KhoiKienThuc": _postprocess_KhoiKienThuc,
}


def build_etl_node_prompts(node_types: Optional[List[str]] = None) -> Dict[str, str]:
	"""Tạo prompt trích xuất theo từng node, bám sát metadata trong XuLyChuongTrinhDaoTao.py."""
	prompts: Dict[str, str] = {
		"VanBanPhapLy": """
Trích xuất node VanBanPhapLy từ văn bản.
Chỉ trả về JSON object dạng: {"node_type":"VanBanPhapLy","items":[...]}
Mỗi item gồm đúng các thuộc tính: so, ten, loai, ngay_ban_hanh, co_quan_ban_hanh, noi_dung_goc.
Không đổi tên thuộc tính. Không thêm thuộc tính khác.
""",
		"TrinhDo": """
Trích xuất node TrinhDo.
JSON bắt buộc: {"node_type":"TrinhDo","items":[{"ten_trinh_do":"..."}]}

Few-shot example:
- Trình độ đại học => {"node_type":"TrinhDo","items":[{"ten_trinh_do":"Đại học"}]}
- Trình độ thạc sĩ => {"node_type":"TrinhDo","items":[{"ten_trinh_do":"Thạc sĩ"}]}
""",
		"Nganh": """
Trích xuất node Nganh.
Mỗi item có đúng: ma_nganh, ten_nganh_vi, ten_nganh_en.
""",
		"Khoa": """
Trích xuất đơn vị quản lý dạng Khoa.
Mỗi item có đúng: ten_khoa.
""",
		"BoMon": """
Trích xuất đơn vị quản lý dạng BoMon.
Mỗi item có đúng: ten_bo_mon.
""",
		"ChuongTrinhDaoTao": """
Trích xuất node ChuongTrinhDaoTao.
Mỗi item gồm đúng: ma_chuong_trinh, khoa, he, ngon_ngu, tong_tin_chi, thoi_gian_dao_tao.
Lưu ý: 
 - khoa là số khóa học (ví dụ 51), không phải tên khoa quản lý.
 - he là hệ đào tạo (ví dụ Đại trà, Chất lượng cao). Nếu không có clc hoặc Chất lượng cao thì mặc định là Đại trà.
 - Nếu he là Đại trà thì ngon_ngu là tiếng Việt, nếu có clc hoặc chất lượng cao thì ngon_ngu là tiếng Anh.

Few-shot example:
- Chương trình đào tạo An toàn thông tin, mã ngành 7480202, tổng tín chỉ 161, thời gian đào tạo 4.5 năm => {"node_type":"ChuongTrinhDaoTao","items":[{"ma_chuong_trinh":"7480202","khoa":51,"he":"Đại trà","ngon_ngu":"Tiếng Việt","tong_tin_chi":161,"thoi_gian_dao_tao":4.5}]}
""",
		"LoaiVanBang": """
Trích xuất node LoaiVanBang với thuộc tính loai_van_bang.

Few-shot example:
- Bằng kỹ sư công nghệ thông tin => {"node_type":"LoaiVanBang","items":[{"loai_van_bang":"Kỹ sư"}]}
""",
		"HinhThucDaoTao": """
Trích xuất node HinhThucDaoTao với thuộc tính ten_hinh_thuc.
""",
		"PhuongThucDaoTao": """
Trích xuất node PhuongThucDaoTao với thuộc tính ten_phuong_thuc.
""",
		"MucTieuDaoTao": """
Trích xuất node MucTieuDaoTao.
Mỗi item gồm đúng: loai, noi_dung. Giá trị loai ưu tiên: "chung" hoặc "cu_the".
""",
		"ViTriViecLam": """
Trích xuất node ViTriViecLam với thuộc tính noi_dung.
""",
		"ChuanThamKhao": """
Trích xuất node ChuanThamKhao.
Mỗi item gồm đúng: noi_dung, link, noi_dung_goc.
""",
		"KhaNangHocTap": """
Trích xuất node KhaNangHocTap với thuộc tính noi_dung.
""",
		"ChuanDauRa": """
Trích xuất node ChuanDauRa.
Mỗi item gồm đúng:  ma_chuan, nhom, loai, noi_dung.
Nếu có mã PLO/CDR thì đặt vào ma_chuan.
""",
		"KhoiKienThuc": """
Trích xuất node KhoiKienThuc trong khung chương trình.
Mỗi item gồm đúng: ma_khoi, ten_khoi, tong_tin_chi, tin_chi_bat_buoc, tin_chi_tu_chon.
""",
		"YeuCauTuChon": """
Trích xuất node YeuCauTuChon thuộc các khối kiến thức.
Mỗi item gồm đúng: noi_dung_yeu_cau, so_tin_chi_yeu_cau.
""",
		"NhomHocPhanTuChon": """
Trích xuất node NhomHocPhanTuChon (nhóm thành phần như AV, PV...).
Mỗi item gồm đúng: ten_nhom.
""",
		"HocPhan": """
Trích xuất node HocPhan.
Mỗi item gồm đúng: ma_hp, ten_hp, so_tin_chi, so_tiet_ly_thuyet, so_tiet_thuc_hanh, bat_buoc.
bat_buoc chỉ nhận true/false/null.
""",
	}

	if not node_types:
		return prompts

	selected: Dict[str, str] = {}
	for node in node_types:
		if node in prompts:
			selected[node] = prompts[node]
	return selected


def _normalize_node_payload(node_type: str, payload: Any) -> Dict[str, Any]:
	"""Chuan hoa ket qua moi node thanh {node_type, items[]} va dien du key theo schema."""
	fields = NODE_FIELD_SCHEMAS.get(node_type, [])
	default_item = {k: None for k in fields}
	processor = NODE_POSTPROCESSORS.get(node_type, _postprocess_default)

	if isinstance(payload, dict):
		items = payload.get("items")
		if isinstance(items, list):
			normalized_items: List[Dict[str, Any]] = []
			for item in items:
				if isinstance(item, dict):
					row = default_item.copy()
					for k in fields:
						if k in item:
							row[k] = item[k]
					normalized_items.append(row)
			normalized_items = processor(normalized_items)
			return {"node_type": node_type, "items": normalized_items}

		# Trường hợp model trả về 1 object thay vì items[].
		row = default_item.copy()
		for k in fields:
			if k in payload:
				row[k] = payload[k]
		normalized_items = processor([row]) if fields else []
		return {"node_type": node_type, "items": normalized_items}

	return {"node_type": node_type, "items": []}


def build_relationship_prompt() -> str:
	"""Prompt trích xuất quan hệ (relationship) giữa các node đã chuẩn hóa."""
	rel_list = ", ".join(RELATION_TYPES)
	return f"""
Trích xuất relationship giữa các node trong tài liệu CTĐT.
Chỉ được dùng rel_type nằm trong danh sách: {rel_list}

Trả về DUY NHẤT JSON hợp lệ theo định dạng:
{{
  "items": [
    {{
      "source_node_type": "ChuongTrinhDaoTao",
      "source_match": "chuỗi nhận diện nguồn",
      "rel_type": "THUOC_VE",
      "target_node_type": "Nganh",
      "target_match": "chuỗi nhận diện đích",
      "evidence": "câu hoặc cụm trích dẫn ngắn"
    }}
  ]
}}

Ràng buộc:
- Không đổi tên trường.
- Không thêm trường lạ.
- Nếu không suy ra quan hệ thì trả {{"items": []}}.
"""


def _normalize_relationships(payload: Any) -> Dict[str, Any]:
	if not isinstance(payload, dict):
		return {"items": []}

	items = payload.get("items")
	if not isinstance(items, list):
		return {"items": []}

	normalized: List[Dict[str, Any]] = []
	for item in items:
		if not isinstance(item, dict):
			continue
		rel_type = _clean_text(item.get("rel_type"))
		if rel_type not in RELATION_TYPES:
			continue
		normalized.append(
			{
				"source_node_type": _clean_text(item.get("source_node_type")),
				"source_match": _clean_text(item.get("source_match")),
				"rel_type": rel_type,
				"target_node_type": _clean_text(item.get("target_node_type")),
				"target_match": _clean_text(item.get("target_match")),
				"evidence": _clean_text(item.get("evidence")),
			}
		)

	return {"items": normalized}


async def extract_relationships(
	text: str,
	source: Optional[str] = None,
	temperature: float = 0.1,
	max_text_chars: int = 18000,
	llm_retries: int = 2,
) -> Dict[str, Any]:
	"""Trích xuất relationship từ text markdown."""
	prompt = build_relationship_prompt()
	raw = await extract_entities(
		text=text,
		prompt=prompt,
		source=source,
		temperature=temperature,
		optimize_calls=False,
		fallback_per_prompt=True,
		max_text_chars=max_text_chars,
		llm_retries=llm_retries,
	)

	rel = _normalize_relationships(raw)
	if isinstance(raw, dict) and raw.get("source"):
		rel["source"] = raw["source"]
	return rel


async def extract_entities(
	text: str,
	prompt: Union[str, List[str], Dict[str, str]],
	source: Optional[str] = None,
	temperature: float = 0.2,
	optimize_calls: bool = True,
	fallback_per_prompt: bool = True,
	max_text_chars: int = 18000,
	llm_retries: int = 2,
	fallback_parallel_limit: int = 3,
	per_prompt_parallel_limit: int = 3,
) -> Dict[str, Any]:
	"""
	Trích xuất entities từ text markdown bằng LLM 7B.

	Args:
		text: Đoạn văn bản markdown.
		prompt: Một prompt (str), nhiều prompt (list[str]) hoặc map {ten_node: prompt}.
		source: Tên file hoặc đường dẫn file nguồn.
		temperature: Nhiệt độ khi gọi model.
		optimize_calls: True => gom nhiều prompt vào 1 lần gọi LLM.
		fallback_per_prompt: Nếu gọi gom lỗi JSON thì gọi từng prompt.
		max_text_chars: Giới hạn độ dài text gửi LLM để tránh timeout.
		llm_retries: Số lần retry nếu request LLM timeout.
		fallback_parallel_limit: Số request song song tối đa trong fallback.
		per_prompt_parallel_limit: Số request song song tối đa khi gọi từng prompt.

	Returns:
		- Nếu prompt là str: trả về JSON dict cho prompt đó.
		- Nếu prompt là list/dict: trả về {"source": "<filename>", "results": {<key>: <json>}}.
	"""

	source_name = Path(source).name if source else None
	text_for_llm = (text or "")[:max_text_chars]
	if len(text or "") > max_text_chars:
		text_for_llm += "\n\n[TRUNCATED]"

	def _parse_json(raw_text: str) -> Dict[str, Any]:
		raw = (raw_text or "").strip()
		if not raw:
			return {}
		try:
			parsed = json.loads(raw)
			if isinstance(parsed, dict):
				return parsed
		except Exception:
			pass

		match = re.search(r"\{[\s\S]*\}", raw)
		if not match:
			return {}

		try:
			parsed = json.loads(match.group(0))
			if isinstance(parsed, dict):
				return parsed
		except Exception:
			return {}

		return {}

	def _build_request(extract_prompt: str) -> str:
		return f"""
		Bạn là bộ trích xuất thực thể cho hệ thống tư vấn đào tạo CTU.
		Chỉ trả về DUY NHẤT JSON hợp lệ, không thêm giải thích.

TEXT (markdown):
\"\"\"
{text_for_llm}
\"\"\"

YÊU CẦU TRÍCH XUẤT:
{extract_prompt}
"""

	async def _call_model_with_retry(llm_prompt: str) -> str:
		last_error: Optional[Exception] = None
		last_error_text = ""
		for attempt in range(llm_retries + 1):
			try:
				return await call_model_7b(llm_prompt, temperature=temperature)
			except Exception as e:
				last_error = e
				last_error_text = f"{type(e).__name__}: {repr(e)}"
				is_timeout = (
					"readtimeout" in str(e).lower()
					or "timeout" in str(e).lower()
					or type(e).__name__.lower().endswith("timeout")
				)
				if (not is_timeout) or attempt >= llm_retries:
					break
				print(f"[extract_entities] Retry {attempt + 1}/{llm_retries} after timeout: {last_error_text}")
				await asyncio.sleep(1.2 * (attempt + 1))

		if last_error is not None:
			print(f"[extract_entities] LLM call failed: {last_error_text}")
		return "{}"

	if isinstance(prompt, str):
		prompt_map = {"result": prompt.strip()}
		single_mode = True
	elif isinstance(prompt, list):
		prompt_map = {
			f"prompt_{idx + 1}": p.strip()
			for idx, p in enumerate(prompt)
			if isinstance(p, str) and p.strip()
		}
		single_mode = False
	elif isinstance(prompt, dict):
		prompt_map = {
			str(k).strip(): str(v).strip()
			for k, v in prompt.items()
			if str(k).strip() and isinstance(v, str) and v.strip()
		}
		single_mode = False
	else:
		return {"source": source_name} if source_name else {}

	if not prompt_map:
		return {"source": source_name} if source_name else {}

	if single_mode or not optimize_calls or len(prompt_map) == 1:
		results: Dict[str, Any] = {}

		if single_mode or len(prompt_map) == 1:
			for key, p in prompt_map.items():
				request = _build_request(p)
				raw = await _call_model_with_retry(request)
				results[key] = _parse_json(raw)
		else:
			sem = asyncio.Semaphore(max(1, per_prompt_parallel_limit))

			async def _extract_one_prompt(key: str, p: str):
				request = _build_request(p)
				async with sem:
					one_raw = await _call_model_with_retry(request)
				return key, _parse_json(one_raw)

			tasks = [_extract_one_prompt(key, p) for key, p in prompt_map.items()]
			gathered = await asyncio.gather(*tasks, return_exceptions=True)
			for item in gathered:
				if isinstance(item, Exception):
					continue
				k, v = item
				results[k] = v

			for key in prompt_map.keys():
				results.setdefault(key, {})

		if single_mode:
			single_result = results.get("result", {}) if isinstance(results.get("result", {}), dict) else {}
			if source_name:
				single_result["source"] = source_name
			return single_result

		response_obj: Dict[str, Any] = {"results": results}
		if source_name:
			response_obj["source"] = source_name
		return response_obj

	prompt_lines = "\n".join([f"- {k}: {v}" for k, v in prompt_map.items()])
	batch_request = f"""
		Bạn là bộ trích xuất thực thể cho hệ thống tư vấn đào tạo CTU.
		Với mỗi mục trong danh sách yêu cầu, hãy trích xuất từ TEXT và trả về JSON.
		Chỉ trả về DUY NHẤT một JSON hợp lệ, không thêm giải thích.

TEXT (markdown):
\"\"\"
{text_for_llm}
\"\"\"

YÊU CẦU THEO TỪNG NODE:
{prompt_lines}

ĐỊNH DẠNG BẮT BUỘC:
{{
  "results": {{
    "ten_node_1": {{...}},
    "ten_node_2": {{...}}
  }}
}}
Trong đó key phải trùng khớp 100% với danh sách yêu cầu.
"""

	raw = await _call_model_with_retry(batch_request)
	parsed = _parse_json(raw)

	if isinstance(parsed.get("results"), dict):
		response_obj: Dict[str, Any] = {"results": parsed["results"]}
		if source_name:
			response_obj["source"] = source_name
		return response_obj

	if fallback_per_prompt:
		sem = asyncio.Semaphore(max(1, fallback_parallel_limit))

		async def _extract_one(key: str, p: str):
			request = _build_request(p)
			async with sem:
				one_raw = await _call_model_with_retry(request)
			return key, _parse_json(one_raw)

		tasks = [_extract_one(key, p) for key, p in prompt_map.items()]
		gathered = await asyncio.gather(*tasks, return_exceptions=True)

		results: Dict[str, Any] = {}
		for item in gathered:
			if isinstance(item, Exception):
				continue
			k, v = item
			results[k] = v

		for key in prompt_map.keys():
			results.setdefault(key, {})

		response_obj: Dict[str, Any] = {"results": results}
		if source_name:
			response_obj["source"] = source_name
		return response_obj

	response_obj: Dict[str, Any] = {"results": {}}
	if source_name:
		response_obj["source"] = source_name
	return response_obj


async def extract_etl_nodes(
	text: str,
	source: Optional[str] = None,
	node_types: Optional[List[str]] = None,
	temperature: float = 0.1,
	optimize_calls: bool = False,
	fallback_per_prompt: bool = True,
	include_relationships: bool = True,
	max_text_chars: int = 12000,
	llm_retries: int = 3,
	fallback_parallel_limit: int = 2,
	per_prompt_parallel_limit: int = 3,
) -> Dict[str, Any]:
	"""Trích xuất node ETL và tùy chọn trích xuất relationship."""
	prompts = build_etl_node_prompts(node_types=node_types)
	raw = await extract_entities(
		text=text,
		prompt=prompts,
		source=source,
		temperature=temperature,
		optimize_calls=optimize_calls,
		fallback_per_prompt=fallback_per_prompt,
		max_text_chars=max_text_chars,
		llm_retries=llm_retries,
		fallback_parallel_limit=fallback_parallel_limit,
		per_prompt_parallel_limit=per_prompt_parallel_limit,
	)

	raw_results = raw.get("results", {}) if isinstance(raw, dict) else {}
	normalized_results: Dict[str, Any] = {}
	for node_type in prompts.keys():
		normalized_results[node_type] = _normalize_node_payload(node_type, raw_results.get(node_type, {}))

	response: Dict[str, Any] = {"results": normalized_results}
	if isinstance(raw, dict) and raw.get("source"):
		response["source"] = raw["source"]

	if include_relationships:
		rels = await extract_relationships(
			text=text,
			source=source,
			temperature=temperature,
			max_text_chars=max_text_chars,
			llm_retries=llm_retries,
		)
		response["relationships"] = rels.get("items", [])
	return response

def pdf_to_markdown(pdf_path: str) -> str:
	import pymupdf4llm
	return pymupdf4llm.to_markdown(pdf_path)


def save_json_output(data: Dict[str, Any], output_path: str) -> str:
	"""Lưu kết quả JSON ra file và trả về đường dẫn đã lưu."""
	path = Path(output_path)
	path.parent.mkdir(parents=True, exist_ok=True)
	with path.open("w", encoding="utf-8") as f:
		json.dump(data, f, ensure_ascii=False, indent=2)
	return str(path)

if __name__ == "__main__":
	SAMPLE_PDF = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
	OUTPUT_JSON = "backend/data/processed/extracted_entities_demo.json"
	try:
		SAMPLE_TEXT = pdf_to_markdown(SAMPLE_PDF)
	except ModuleNotFoundError:
		SAMPLE_TEXT = "Nganh An toan thong tin ma nganh 7480202, tong tin chi 161, thoi gian dao tao 4.5 nam."
	result = asyncio.run(extract_etl_nodes(SAMPLE_TEXT, source=SAMPLE_PDF))
	saved_path = save_json_output(result, OUTPUT_JSON)
	print(result)
	print(f"Saved JSON: {saved_path}")
