from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EvaluationResult:
    is_enough: bool
    reason: str
    action: str


class EvaluatorService:
    """Evaluate retrieval context quality and suggest next action."""

    async def check(self, context: str) -> EvaluationResult:
        ctx = (context or "").strip()
        if not ctx:
            return EvaluationResult(False, "empty_context", "more_context")

        short_context = len(ctx) < 80
        low_signal_terms = ["không tìm thấy", "no result", "none"]
        low_signal = any(term in ctx.lower() for term in low_signal_terms)
        if short_context or low_signal:
            return EvaluationResult(False, "insufficient_context", "rewrite")

        return EvaluationResult(True, "context_ok", "none")

    async def need_more_context(self, result: EvaluationResult) -> bool:
        return result.action == "more_context"

    async def need_rewrite(self, result: EvaluationResult) -> bool:
        return result.action == "rewrite"

    async def need_switch(self, result: EvaluationResult) -> bool:
        return result.action == "switch"
