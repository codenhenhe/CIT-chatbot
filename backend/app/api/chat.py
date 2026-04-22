from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
import json
from fastapi.responses import StreamingResponse
from app.services.chat_service import ChatService

router = APIRouter()
chat_service = ChatService()

class MessagePart(BaseModel):
    type: str = "text"
    text: str


class MessageItem(BaseModel):
    role: str
    parts: List[MessagePart]


class ChatRequest(BaseModel):
    # New schema
    message: Optional[str] = None
    history: List[dict] = []
    # Backward-compatible schema from frontend stream payload
    messages: Optional[List[MessageItem]] = None

@router.post("/chat")
async def chat(req: ChatRequest):
    message = (req.message or "").strip()
    history = req.history or []

    # Backward compatibility for payload: { messages: [{role, parts:[{text}]}] }
    if not message and req.messages:
        user_messages = [m for m in req.messages if m.role == "user" and m.parts]
        if user_messages:
            message = (user_messages[-1].parts[0].text or "").strip()

        history = []
        for m in req.messages[:-1]:
            content = ""
            if m.parts:
                content = (m.parts[0].text or "").strip()
            if content:
                history.append({"role": m.role, "content": content})

    result = await chat_service.handle_query(message, history)
    answer_text = str(result.get("answer", "")).strip()
    if not answer_text:
        answer_text = "Xin loi, he thong chua tao duoc cau tra loi."

    async def stream_results():
        # Vercel AI data stream format: each text chunk starts with "0:".
        yield f"0:{json.dumps(answer_text)}\n"

    return StreamingResponse(
        stream_results(),
        media_type="text/plain",
        headers={"X-Vercel-AI-Data-Stream": "v1"},
    )