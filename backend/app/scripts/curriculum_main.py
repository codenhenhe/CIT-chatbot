import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if __package__ in (None, ""):
    backend_root = Path(__file__).resolve().parents[2]
    if str(backend_root) not in sys.path:
        sys.path.append(str(backend_root))
    from app.scripts.curriculum_extractor import extract_curriculum_entities, save_json_output
    from app.scripts.curriculum_parser import PDFParser
else:
    from .curriculum_extractor import extract_curriculum_entities, save_json_output
    from .curriculum_parser import PDFCurriculumParser


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_output_json_path(pdf_path: str) -> str:
    backend_root = Path(__file__).resolve().parents[2]
    output_dir = backend_root / "processed_data" / "json"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir / f"{Path(pdf_path).stem}.json")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return _clean_text(value)


def _build_fallback_text(node_type: str, item: Dict[str, Any]) -> str:
    details: List[str] = []
    for key, value in item.items():
        if key in {"text", "embedding"}:
            continue
        text_val = _clean_text(value)
        if text_val:
            details.append(f"{key}: {text_val}")
    if details:
        return f"{node_type} | " + " ; ".join(details)
    return node_type


def _collect_text_targets(payload: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, Any]]]:
    texts: List[str] = []
    targets: List[Dict[str, Any]] = []

    results = payload.get("results", {})
    if not isinstance(results, dict):
        return texts, targets

    for node_type, block in results.items():
        if not isinstance(block, dict):
            continue
        items = block.get("items", [])
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            text = _clean_text(item.get("text"))
            if not text:
                text = _build_fallback_text(node_type, item)
                item["text"] = text
            texts.append(text)
            targets.append(item)

    return texts, targets


def _attach_embeddings_to_payload(payload: Dict[str, Any]) -> int:
    texts, targets = _collect_text_targets(payload)
    if not texts:
        return 0

    if __package__ in (None, ""):
        from app.scripts.Embedding import get_embedding_model
    else:
        from .Embedding import get_embedding_model

    embedder = get_embedding_model()
    embeddings = embedder.get_embedding_batch(texts)
    if len(embeddings) != len(targets):
        raise RuntimeError("So luong embedding khong khop so luong node item.")

    for item, emb in zip(targets, embeddings):
        item["embedding"] = [float(dim) for dim in emb]
    return len(targets)


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

    embedded_items = 0
    if approved:
        embedded_items = _attach_embeddings_to_payload(payload)

    payload.setdefault("admin_review", {})
    payload["admin_review"].update(
        {
            "approved": approved,
            "reviewed_by": reviewed_by,
            "reviewed_at": _utc_now_iso(),
            "notes": notes,
            "embedding_generated": approved,
            "embedded_items": embedded_items,
        }
    )
    save_json_output(_json_safe(payload), json_path)
    return payload


if __name__ == "__main__":
    pdf_path = "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.signed.signed.signed.signed.signed.pdf"
    result = asyncio.run(run_curriculum_etl(pdf_path))
    print(json.dumps({"source": result.get("source"), "status": result.get("status")}, ensure_ascii=False))
