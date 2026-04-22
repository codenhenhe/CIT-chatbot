from __future__ import annotations


class StrategyService:
    """Map domain + intent to retrieval strategy."""

    async def select(self, domain: str, intent: str) -> str:
        if domain == "quy_che":
            return "hybrid"

        mapping = {
            "factual": "graph",
            "relational": "graph",
            "rule": "hybrid",
        }
        return mapping.get(intent, "hybrid")

    async def switch(self, current_strategy: str) -> str:
        order = ["vector", "graph", "hybrid"]
        if current_strategy not in order:
            return "hybrid"
        idx = order.index(current_strategy)
        return order[(idx + 1) % len(order)]
