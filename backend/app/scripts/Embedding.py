from typing import List, Union, Optional
from llama_index.core import Settings
from sentence_transformers import SentenceTransformer, util
import torch
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(".env")

# Global embedding model instance - khởi tạo 1 lần
_embedding_model: Optional['EmbeddingModel'] = None


def _find_model_path() -> str:
    """Tìm đường dẫn model local"""
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parents[2]
    model_root = project_root / "my_model_weights" / "bge_m3" / "models--BAAI--bge-m3"
    snapshots_dir = model_root / "snapshots"

    def _is_valid_snapshot(snapshot_dir: Path) -> bool:
        config_path = snapshot_dir / "config.json"
        modules_path = snapshot_dir / "modules.json"
        weights_path = snapshot_dir / "pytorch_model.bin"
        if not (config_path.exists() and modules_path.exists() and weights_path.exists()):
            return False
        try:
            import json
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            return bool(cfg.get("model_type"))
        except Exception:
            return False

    # Ưu tiên commit hash trong refs/main (đúng snapshot mà HF cache đang trỏ tới)
    refs_main = model_root / "refs" / "main"
    if refs_main.exists():
        snapshot_id = refs_main.read_text(encoding="utf-8").strip()
        candidate = snapshots_dir / snapshot_id
        if _is_valid_snapshot(candidate):
            return str(candidate)

    if snapshots_dir.exists():
        valid_snapshots = [p for p in snapshots_dir.glob("*") if _is_valid_snapshot(p)]
        if valid_snapshots:
            return str(sorted(valid_snapshots)[-1])

    raise RuntimeError(
        f"No valid local model snapshot found under {snapshots_dir}. "
        "Please verify bge-m3 files are complete."
    )


def get_embedding_model() -> 'EmbeddingModel':
    """Lấy global embedding model instance (tạo 1 lần duy nhất)"""
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = EmbeddingModel()
    return _embedding_model


class EmbeddingModel:
    def __init__(self, device=None):
        # Tìm đường dẫn model local
        model_path = _find_model_path()
        
        resolved_device = device or os.getenv("EMBEDDING_DEVICE") or self._detect_best_device()
        self.device = resolved_device
        self.model_name = "BAAI/bge-m3"
        self.model_path = model_path
        
        # Load model từ đường dẫn local dùng SentenceTransformer
        self.model = SentenceTransformer(
            model_path,
            device=resolved_device,
            trust_remote_code=True,
            local_files_only=True,
        )
        print(
            f"[Startup] Embedding initialized | model={self.model_name} | device={self.device} | path={self.model_path}"
        )

    @staticmethod
    def _detect_best_device() -> str:
        if torch.cuda.is_available():
            return "cuda"
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            return "mps"
        return "cpu"

    def get_embedding_batch(self, texts: Union[str, List[str]]):
        if isinstance(texts, str):
            texts = [texts]
        
        cleaned_texts = [t.strip() for t in texts if t and t.strip()]
        if not cleaned_texts:
            return []

        embeddings = self.model.encode(cleaned_texts, convert_to_tensor=False)
        return [list(emb) if hasattr(emb, '__iter__') else emb for emb in embeddings]
    
    def get_similarity(self, text_a: str, text_b: str):
        """So sánh nhanh 2 câu văn bản"""
        vec_a, vec_b = self.get_embedding_batch([text_a, text_b])
        # util.cos_sim trả về một ma trận, .item() để lấy giá trị số duy nhất
        return util.cos_sim(vec_a, vec_b).item()

    def find_best_match(self, query: str, document_list: List[str], top_k=3):
        """Tìm top K đoạn văn bản giống với câu hỏi nhất"""
        query_vec = self.get_embedding_batch([query])[0]
        doc_vecs = self.get_embedding_batch(document_list)
        
        # Tính toán độ tương đồng 1 lúc cho cả danh sách
        cos_scores = util.cos_sim(query_vec, doc_vecs)[0]
        
        # Lấy top k kết quả cao nhất
        top_results = torch.topk(cos_scores, k=min(top_k, len(document_list)))
        
        return top_results
    
# emb = EmbeddingModel()
# texts = ["Hello world!", "This is a test.", "   ", "", "Another sentence.  "]
# embeddings = emb.get_embedding_batch(texts)
# cos = emb.get_similarity("Giỏi lắm", "Tệ thật")
# print(embeddings[3])
# print(len(embeddings))
# print(cos)

# query = "Đại học Cần Thơ nằm ở đâu?"
# ans = [
#     "Đại học Cần Thơ nằm ở thành phố Cần Thơ, Việt Nam.",
#     "Đại học Cần Thơ là một trong những trường đại học lớn nhất ở miền Tây Việt Nam.",
#     "Đại học Cần Thơ có nhiều ngành đào tạo khác nhau.",
#     "Đại học Cần Thơ được thành lập vào năm 1966."
# ]

# results = emb.find_best_match(query, ans, top_k=2)
# scores, indices = results.values, results.indices
# print(scores)
# print(indices)
# for score, idx in zip(scores, indices):
#     print(f"Score: {score:.4f}, Text: {ans[idx]}")