import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from dotenv import load_dotenv
from app.api.deps import require_admin
from app.scripts.curriculum_graph import import_json_to_neo4j
from app.scripts.neo4j_class import Neo4jConnector
from app.scripts.curriculum_main import mark_admin_review, run_curriculum_etl
from app.scripts.run import process_quy_che_hoc_vu

router = APIRouter()

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
UPLOAD_DIR = Path(__file__).resolve().parents[3] / "uploads"
BACKEND_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(BACKEND_ROOT / ".env")

LABELS_TO_INDEX = [
    "ChuongTrinhDaoTao",
    "HocPhan",
    "HinhThucDaoTao",
    "LoaiVanBang",
    "PhuongThucDaoTao",
    "TrinhDo",
    "Khoa",
    "BoMon",
    "Nganh",
    "VanBanPhapLy",
    "ViTriViecLam",
    "MucTieuDaoTao",
    "ChuanDauRa",
    "KhaNangHocTap",
    "KhoiKienThuc",
    "ChuanThamKhao",
    "NhomHocPhanTuChon",
    "YeuCauTuChon",
]

ALLOWED_CATEGORIES = {
    "chuyen_nganh_dao_tao": "ChuyenNganh_DaoTao",
    "quy_che_hoc_vu": "QuyChe_HocVu",
    "huong_dan_thu_tuc": "HuongDan_ThuTuc",
    "thong_bao_ke_hoach": "ThongBao_KeHoach",
}

ingestion_queue: asyncio.Queue = asyncio.Queue()
ingestion_jobs: Dict[str, dict] = {}
ingestion_worker_task: asyncio.Task | None = None


class BaseCategoryProcessor:
    async def process(self, file_path: str) -> dict:
        raise NotImplementedError


class ChuyenNganhDaoTaoProcessor(BaseCategoryProcessor):
    async def process(self, file_path: str) -> dict:
        from app.scripts.curriculum_graph import _default_json_path_from_pdf

        json_path = _default_json_path_from_pdf(file_path)
        extracted = await run_curriculum_etl(pdf_path=file_path, output_json=json_path)

        parser_output = extracted.get("parser_output", {}) if isinstance(extracted, dict) else {}
        raw_preview = ""
        if isinstance(extracted, dict):
            raw_preview = json.dumps(extracted, ensure_ascii=False)[:12000]

        return {
            "message": "Đã trích xuất JSON, chờ admin xác nhận trước khi nạp vào Neo4j",
            "ingestion_applied": False,
            "json_path": json_path,
            "section_count": parser_output.get("section_count"),
            "extracted_preview": raw_preview,
        }


class QuyCheHocVuProcessor(BaseCategoryProcessor):
    async def process(self, file_path: str) -> dict:
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD")

        if not neo4j_password:
            raise RuntimeError("Missing NEO4J_PASSWORD in backend .env")

        db = None
        try:
            db = Neo4jConnector(neo4j_uri, neo4j_user, neo4j_password)
            result = await process_quy_che_hoc_vu(file_path, db)
            return result
        finally:
            if db:
                await db.close()


class HuongDanThuTucProcessor(BaseCategoryProcessor):
    async def process(self, file_path: str) -> dict:
        return {
            "message": "Da luu file Huong dan thu tuc. Class xu ly thuc te se duoc bo sung sau.",
            "ingestion_applied": False,
            "saved_file": file_path,
        }


class ThongBaoKeHoachProcessor(BaseCategoryProcessor):
    async def process(self, file_path: str) -> dict:
        return {
            "message": "Da luu file Thong bao ke hoach. Class xu ly thuc te se duoc bo sung sau.",
            "ingestion_applied": False,
            "saved_file": file_path,
        }


PROCESSOR_CLASS_MAP = {
    "chuyen_nganh_dao_tao": ChuyenNganhDaoTaoProcessor,
    "quy_che_hoc_vu": QuyCheHocVuProcessor,
    "huong_dan_thu_tuc": HuongDanThuTucProcessor,
    "thong_bao_ke_hoach": ThongBaoKeHoachProcessor,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_unique_filename(original_name: str) -> str:
    base = Path(original_name).name
    suffix = Path(base).suffix.lower()
    stem = Path(base).stem
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short_id = uuid4().hex[:8]
    return f"{stem}_{timestamp}_{short_id}{suffix}"


async def _run_ingestion_pipeline(file_path: str, category: str) -> dict:
    processor_cls = PROCESSOR_CLASS_MAP.get(category)
    if not processor_cls:
        raise RuntimeError(f"No processor configured for category: {category}")

    processor = processor_cls()
    return await processor.process(file_path)


async def _ingestion_worker() -> None:
    while True:
        job = await ingestion_queue.get()
        job_id = job["job_id"]
        try:
            ingestion_jobs[job_id]["status"] = "processing"
            ingestion_jobs[job_id]["started_at"] = _utc_now_iso()

            result = await _run_ingestion_pipeline(job["saved_to"], job["category"])

            ingestion_jobs[job_id]["status"] = "completed"
            ingestion_jobs[job_id]["completed_at"] = _utc_now_iso()
            ingestion_jobs[job_id]["result"] = result
        except Exception as exc:
            ingestion_jobs[job_id]["status"] = "failed"
            ingestion_jobs[job_id]["completed_at"] = _utc_now_iso()
            ingestion_jobs[job_id]["error"] = str(exc)
        finally:
            ingestion_queue.task_done()


async def start_ingestion_worker() -> None:
    global ingestion_worker_task
    if ingestion_worker_task is None or ingestion_worker_task.done():
        ingestion_worker_task = asyncio.create_task(_ingestion_worker())


async def stop_ingestion_worker() -> None:
    global ingestion_worker_task
    if ingestion_worker_task and not ingestion_worker_task.done():
        ingestion_worker_task.cancel()
        try:
            await ingestion_worker_task
        except asyncio.CancelledError:
            pass
    ingestion_worker_task = None


class JsonUpdatePayload(BaseModel):
    data: dict


class ConfirmImportResponse(BaseModel):
    job_id: str
    json_path: str
    message: str
    ingestion_applied: bool
    nodes: int
    edges: int

@router.post("/upload")
async def upload(
    file: UploadFile = File(...),
    category: str = Form(...),
    user=Depends(require_admin),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="File name is missing")

    if category not in ALLOWED_CATEGORIES:
        raise HTTPException(status_code=400, detail="Invalid category")

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File exceeds 50MB limit")

    category_dir = UPLOAD_DIR / ALLOWED_CATEGORIES[category]
    category_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _build_unique_filename(file.filename)
    save_path = category_dir / safe_name
    save_path.write_bytes(content)

    job_id = uuid4().hex
    ingestion_jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "filename": file.filename,
        "category": category,
        "stored_filename": safe_name,
        "size": len(content),
        "saved_to": str(save_path),
        "uploaded_by": user.get("sub", "unknown"),
        "created_at": _utc_now_iso(),
        "started_at": None,
        "completed_at": None,
        "result": None,
        "error": None,
    }

    await start_ingestion_worker()
    await ingestion_queue.put({"job_id": job_id, "saved_to": str(save_path), "category": category})

    return {
        "msg": "Upload thanh cong, da dua vao hang doi ingestion",
        "job_id": job_id,
        "status": "queued",
        "filename": file.filename,
        "category": category,
        "stored_filename": safe_name,
        "size": len(content),
        "saved_to": str(save_path),
        "queue_size": ingestion_queue.qsize(),
        "uploaded_by": user.get("sub", "unknown"),
    }


@router.get("/upload/status/{job_id}")
async def upload_status(job_id: str, user=Depends(require_admin)):
    job = ingestion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def _get_job_json_path(job: dict) -> Path:
    result = job.get("result") or {}
    json_path = result.get("json_path")
    if not json_path:
        raise HTTPException(status_code=404, detail="No extracted JSON for this job")

    path = Path(json_path).resolve()
    allowed_root = (BACKEND_ROOT / "processed_data" / "json").resolve()
    if allowed_root not in path.parents and path != allowed_root:
        raise HTTPException(status_code=400, detail="Invalid JSON path")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Extracted JSON file not found")
    return path


@router.get("/upload/json/{job_id}")
async def get_extracted_json(job_id: str, user=Depends(require_admin)):
    job = ingestion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    path = _get_job_json_path(job)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {
        "job_id": job_id,
        "json_path": str(path),
        "data": data,
    }


@router.put("/upload/json/{job_id}")
async def update_extracted_json(job_id: str, payload: JsonUpdatePayload, user=Depends(require_admin)):
    job = ingestion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    path = _get_job_json_path(job)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload.data, f, ensure_ascii=False, indent=2)

    return {
        "job_id": job_id,
        "json_path": str(path),
        "message": "Updated extracted JSON successfully",
    }


@router.post("/upload/confirm/{job_id}", response_model=ConfirmImportResponse)
async def confirm_json_and_import(job_id: str, user=Depends(require_admin)):
    job = ingestion_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("category") != "chuyen_nganh_dao_tao":
        raise HTTPException(status_code=400, detail="Category does not support Neo4j confirmation import")

    path = _get_job_json_path(job)

    reviewer = user.get("sub", "admin") if isinstance(user, dict) else "admin"
    mark_admin_review(
        json_path=str(path),
        approved=True,
        reviewed_by=reviewer,
        notes="Confirmed by admin for Neo4j import",
    )

    import_summary = await import_json_to_neo4j(str(path))

    current_result = job.get("result") or {}
    current_result.update(
        {
            "ingestion_applied": True,
            "message": "Da xac nhan va nap du lieu vao Neo4j thanh cong",
            "nodes": import_summary.get("node_count", 0),
            "edges": import_summary.get("edge_count", 0),
            "json_path": import_summary.get("json_path", str(path)),
        }
    )
    job["result"] = current_result
    job["status"] = "completed"
    job["confirmed_at"] = _utc_now_iso()

    return {
        "job_id": job_id,
        "json_path": str(path),
        "message": "Đã xác nhận và nạp Neo4j thành công",
        "ingestion_applied": True,
        "nodes": import_summary.get("node_count", 0),
        "edges": import_summary.get("edge_count", 0),
    }