from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger("app.evaluator")

@dataclass
class EvaluationResult:
    is_enough: bool
    reason: str
    action: str  # actions: "none", "rewrite", "more_context", "switch"

class EvaluatorService:
    def __init__(self, llm_service: Optional[Any] = None) -> None:
        self.llm_service = llm_service

    async def check(self, query: str, context: str, cypher: str = "") -> EvaluationResult:
        ctx = (context or "").strip()
        
        # Bước 1: Heuristic check (Vẫn giữ để thoát nhanh nếu rỗng)
        if not ctx:
            return EvaluationResult(False, "Dữ liệu trống hoàn toàn", "more_context")
            
        # Bước 2: Đánh giá bằng LLM với đầy đủ bằng chứng
        return await self._llm_evaluate(query, ctx, cypher)

    async def _llm_evaluate(self, query: str, context: str, cypher: str) -> EvaluationResult:
        prompt = f"""
        Bạn là Kiểm sát viên dữ liệu cho CICTBot (Đại học Cần Thơ).
        Nhiệm vụ: Xác định xem DỮ LIỆU có trả lời được CÂU HỎI hay không.

        BẰNG CHỨNG HỆ THỐNG:
        - Câu hỏi: "{query}"
        - Truy vấn Database đã dùng: "{cypher}"
        - Kết quả thu được: "{context}"

        QUY TẮC ĐÁNH GIÁ (NGHIÊM NGẶT):
        1. Nếu DỮ LIỆU chứa con số hoặc thông tin khớp với ý định của CÂU HỎI (dù ngắn) -> is_enough: true.
        2. Nếu TRUY VẤN cho thấy đã tìm đúng Thực thể (VD: Ngành KHMT) và DỮ LIỆU trả về thuộc tính của nó -> is_enough: true (Không được bắt retry vì dữ liệu ngắn).
        3. Chỉ trả về is_enough: false nếu DỮ LIỆU hoàn toàn không liên quan hoặc ghi "không tìm thấy".

        TRẢ VỀ JSON:
        {{
            "is_enough": true/false,
            "reason": "Giải thích ngắn",
            "action": "none" | "rewrite" | "more_context"
        }}
        """
        try:
            # Ép dùng duy nhất model 9B để tránh Model Swapping
            res = await self.llm_service.call_model_9b_json(prompt)
            return EvaluationResult(
                is_enough=bool(res.get("is_enough", True)),
                reason=str(res.get("reason", "ok")),
                action=str(res.get("action", "none"))
            )
        except:
            return EvaluationResult(True, "fallback", "none")