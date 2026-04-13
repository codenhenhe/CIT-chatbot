from __future__ import annotations

import logging
from typing import Any, Dict, Optional


logger = logging.getLogger("app.entity")


class EntityService:
    """Extract key entities for curriculum questions using LLM."""

    def __init__(self, llm_service: Optional[object] = None) -> None:
        self.llm_service = llm_service

    async def extract(self, query: str) -> Dict[str, Any]:
        text = (query or "").strip()
        logger.info("[entity] extraction started")

        empty_entities: Dict[str, Any] = {
            "mentions": [],
            "intent_hint": "",
            "relation": None,
        }
        if not text:
            logger.info("[entity] empty query")
            return empty_entities

        if not self.llm_service:
            logger.warning("[entity] llm unavailable, return empty entities")
            return empty_entities

        data = await self.llm_service.extract_entities(text)
        entities = {
            "mentions": data.get("mentions", []),
            "intent_hint": str(data.get("intent_hint", "")).strip(),
            "relation": data.get("relation"),
        }
        logger.info("[entity] extraction done mentions=%s relation=%s", len(entities["mentions"]), entities["relation"])
        return entities
