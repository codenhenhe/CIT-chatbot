from __future__ import annotations

import logging
from typing import Optional


logger = logging.getLogger("app.domain")


class DomainService:
    """Classify incoming query into supported domains."""

    def __init__(self, llm_service: Optional[object] = None) -> None:
        self.llm_service = llm_service

    async def classify(self, query: str) -> str:
        logger.info("[domain] classify started")
        if self.llm_service:
            predicted = await self.llm_service.classify_domain(query)
            if predicted in {"ctdt", "quy_che"}:
                logger.info("[domain] classify=%s", predicted)
                return predicted

        logger.warning("[domain] fallback to ctdt due to empty/invalid prediction")
        return "ctdt"
