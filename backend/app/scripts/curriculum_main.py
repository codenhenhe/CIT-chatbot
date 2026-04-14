import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

if __package__ in (None, ""):
    backend_root = Path(__file__).resolve().parents[2]
    if str(backend_root) not in sys.path:
        sys.path.append(str(backend_root))
    from app.scripts.curriculum_extractor import extract_curriculum_entities, save_json_output
    from app.scripts.curriculum_parser import PDFParser
else:
    from .curriculum_extractor import extract_curriculum_entities, save_json_output
    from .curriculum_parser import PDFParser


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_output_json_path(pdf_path: str) -> str:
    backend_root = Path(__file__).resolve().parents[2]
    output_dir = backend_root / "processed_data" / "json"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir / f"{Path(pdf_path).stem}.json")


async def run_curriculum_etl(pdf_path: str, output_json: Optional[str] = None) -> Dict[str, Any]:
    """Orchestrate parser and extractor, then persist extracted graph-ready JSON."""
    if not output_json:
        output_json = _default_output_json_path(pdf_path)

    parser = PDFParser(pdf_path)
    parser_sections = parser.parse_pdf_by_structure()

    extracted = extract_curriculum_entities(
        parser_text="",
        table_text="",
        tables=None,
        parser_sections=parser_sections,
        source=pdf_path,
    )

    extracted.setdefault(
        "admin_review",
        {
            "approved": False,
            "reviewed_by": None,
            "reviewed_at": None,
            "notes": "",
        },
    )

    save_json_output(extracted, output_json)
    return extracted


def load_admin_json(json_path: str) -> Dict[str, Any]:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def mark_admin_review(
    json_path: str,
    approved: bool,
    reviewed_by: str = "admin",
    notes: str = "",
) -> Dict[str, Any]:
    payload = load_admin_json(json_path)
    payload["status"] = "approved" if approved else "rejected"
    payload.setdefault("admin_review", {})
    payload["admin_review"].update(
        {
            "approved": approved,
            "reviewed_by": reviewed_by,
            "reviewed_at": _utc_now_iso(),
            "notes": notes,
        }
    )
    save_json_output(payload, json_path)
    return payload


if __name__ == "__main__":
    pdf_path = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
    result = asyncio.run(run_curriculum_etl(pdf_path))
    print(json.dumps({"source": result.get("source"), "status": result.get("status")}, ensure_ascii=False))
