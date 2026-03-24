import httpx
import json
import os
import re
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv


def _load_env_files() -> None:
    # Load backend and repo-level env files to support both dev run locations.
    backend_root = Path(__file__).resolve().parents[2]
    repo_root = backend_root.parent
    load_dotenv(backend_root / ".env", override=False)
    load_dotenv(repo_root / ".env", override=False)


def _get_env(*keys: str, default: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return default

_load_env_files()

OLLAMA_BASE_URL = _get_env("OLLAMA_BASE_URL", "OLLAMA_HOST", default="http://localhost:11434").rstrip("/")
OLLAMA_GENERATE_URL = os.getenv("OLLAMA_GENERATE_URL", f"{OLLAMA_BASE_URL}/api/generate")
OLLAMA_CHAT_URL = os.getenv("OLLAMA_CHAT_URL", f"{OLLAMA_BASE_URL}/api/chat")
DEFAULT_TIMEOUT_SECONDS = 60
MODEL_3B = os.getenv("OLLAMA_MODEL_3B", "llama3:3b")
MODEL_7B = os.getenv("OLLAMA_MODEL_7B", "llama3:7b")


def _extract_json_object(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}

    # Try parse raw content first.
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # Try parse fenced code or inline JSON object.
    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        return {}

    try:
        data = json.loads(match.group(0))
        if isinstance(data, dict):
            return data
    except Exception:
        return {}

    return {}


async def call_model(model: str, prompt: str):
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
        try:
            # Preferred endpoint for prompt-style calls.
            res = await client.post(
                OLLAMA_GENERATE_URL,
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                },
            )
            if res.status_code == 404:
                raise httpx.HTTPStatusError("Generate endpoint not found", request=res.request, response=res)
            res.raise_for_status()
            payload = res.json()
            if "response" not in payload:
                raise RuntimeError(f"Invalid Ollama response: {payload}")
            return payload["response"]
        except httpx.HTTPStatusError as e:
            # Fallback for runtimes exposing /api/chat but not /api/generate.
            if e.response is not None and e.response.status_code == 404:
                chat_res = await client.post(
                    OLLAMA_CHAT_URL,
                    json={
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                    },
                )
                chat_res.raise_for_status()
                chat_payload = chat_res.json()
                content = (chat_payload.get("message", {}) or {}).get("content")
                if not content:
                    raise RuntimeError(f"Invalid Ollama chat response: {chat_payload}")
                return content
            raise


async def call_model_json(model: str, prompt: str) -> Dict[str, Any]:
    text = await call_model(model, prompt)
    return _extract_json_object(text)


async def call_model_3b(prompt: str):
    return await call_model(MODEL_3B, prompt)


async def call_model_7b(prompt: str):
    return await call_model(MODEL_7B, prompt)


async def call_model_3b_json(prompt: str) -> Dict[str, Any]:
    return await call_model_json(MODEL_3B, prompt)