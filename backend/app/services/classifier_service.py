from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, Optional


logger = logging.getLogger("app.intent")


class IntentClassifierService:
    """Classify CTDT intent with LLM-first approach."""

    VALID_INTENTS = {"factual", "relational", "rule"}

    def __init__(self, llm_service: Optional[object] = None) -> None:
        self.llm_service = llm_service
        self.timeout_seconds = float(os.getenv("INTENT_CLASSIFY_TIMEOUT_SECONDS", "20"))

    async def classify(
        self,
        query: str,
        entities: Optional[Dict[str, str]] = None,
        domain: str = "ctdt",
    ) -> str:
        _ = entities
        logger.info("[intent] classify started domain=%s", domain)

        if domain == "quy_che":
            logger.info("[intent] force rule for quy_che domain")
            return "rule"

        if not self.llm_service:
            logger.warning("[intent] llm unavailable, fallback factual")
            return "factual"

        try:
            predicted = await asyncio.wait_for(self.llm_service.classify_intent(query), timeout=self.timeout_seconds)
        except TimeoutError:
            logger.error("[intent] timeout after %ss, fallback factual", self.timeout_seconds)
            return "factual"
        except Exception as exc:
            logger.error("[intent] classify failed: %s", exc)
            return "factual"

        if predicted in self.VALID_INTENTS:
            logger.info("[intent] classify=%s", predicted)
            return predicted

        logger.warning("[intent] invalid prediction '%s', fallback factual", predicted)
        return "factual"
