from typing import List, Union
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Settings
from sentence_transformers import util
import torch
import os
from dotenv import load_dotenv

load_dotenv(".env")
# cache_folder="../my_model_weights/bge_m3",
class EmbeddingModel:
    def __init__(self, model_name="BAAI/bge-m3", cache_folder="../my_model_weights/bge_m3", device=None, embed_batch_size=2):
        # Ưu tiên GPU nếu có, fallback về CPU để tránh crash khi không có CUDA/MPS
        resolved_device = device or os.getenv("EMBEDDING_DEVICE") or self._detect_best_device()
        self.device = resolved_device
        self.model_name = model_name
        self.cache_folder = cache_folder
        self.model = HuggingFaceEmbedding(
            model_name=model_name,
            cache_folder=cache_folder, 
            device=resolved_device,
            embed_batch_size=embed_batch_size,
            token = os.getenv("HF_TOKEN") 
        )
        
        # Cập nhật Settings toàn cục để các hàm khác của LlamaIndex dùng chung
        Settings.embed_model = self.model
        print(
            f"[Startup] Embedding initialized | model={self.model_name} | device={self.device} | cache_folder={self.cache_folder}"
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

        return self.model.get_text_embedding_batch(cleaned_texts)
    
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