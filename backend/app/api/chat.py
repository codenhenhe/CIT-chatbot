from fastapi import APIRouter
import re
import os
from app.scripts.prototype import GraphRAGChatbot
from pydantic import BaseModel
from typing import List, Optional
import ollama
import json
from fastapi.responses import StreamingResponse

router = APIRouter()

bot = GraphRAGChatbot()

class MessagePart(BaseModel):
    type: str
    text: str

class MessageItem(BaseModel):
    id: Optional[str] = None
    role: str
    parts: List[MessagePart]

class ChatRequest(BaseModel):
    id: Optional[str] = None
    messages: List[MessageItem]
    trigger: Optional[str] = None

ROUTER_MODEL = "qwen2.5-coder:1.5b"  
# FINAL_MODEL = "qwen2.5-coder:3b-instruct" 
FINAL_MODEL = "qwen2.5-coder:7b" 


def build_ollama_options():
    options = {"temperature": 0.0}
    num_gpu = os.getenv("OLLAMA_NUM_GPU")
    if num_gpu is not None and num_gpu != "":
        options["num_gpu"] = int(num_gpu)
    return options

def detect_intent(query: str):
    q = query.lower().strip()

    if len(q) < 3:
        return "SMALL_TALK"

    # ===== BASIC =====
    if re.fullmatch(r"(xin chào|chào|hi|hello|hey)+", q):
        return "GREETING"

    if any(x in q for x in ["cảm ơn", "thanks"]):
        return "THANKS"

    if any(x in q for x in ["tạm biệt", "bye"]):
        return "GOODBYE"

    # ===== FEEDBACK =====
    if any(x in q for x in ["sai", "sai rồi", "không đúng", "nhầm"]):
        return "CORRECTION"

    if any(x in q for x in ["ý tôi là", "tôi muốn hỏi", "ý mình là"]):
        return "REFINE"

    # ===== CTU DOMAIN =====
    if any(x in q for x in ["bao nhiêu tín chỉ", "tín chỉ"]):
        return "CREDIT"

    if any(x in q for x in ["môn học", "học phần"]):
        return "COURSE"

    if any(x in q for x in ["chương trình đào tạo", "ctdt"]):
        return "CURRICULUM"

    if any(x in q for x in ["ngành", "chuyên ngành"]):
        return "MAJOR"

    if any(x in q for x in ["học phí"]):
        return "TUITION"

    if any(x in q for x in ["điều kiện", "yêu cầu", "đầu vào"]):
        return "REQUIREMENT"

    if any(x in q for x in ["ở đâu", "khoa nào", "thuộc khoa"]):
        return "FACULTY"

    # ===== fallback =====
    return "RAG"

async def rewrite_query(client, history, query, ollama_options):
    # chặn câu quá ngắn
    if len(query.strip()) < 5:
        return query

    prompt = (
        "Nhiệm vụ: Viết lại câu hỏi người dùng cho rõ nghĩa hơn.\n"
        "- KHÔNG được trả lời.\n"
        "- KHÔNG thêm thông tin mới.\n"
        "- KHÔNG suy đoán.\n"
        "- Chỉ output duy nhất câu hỏi đã rewrite.\n"
        "- Nếu câu đã rõ → giữ nguyên.\n"
    )

    messages = history[-3:] + [{"role": "user", "content": query}]

    resp = await client.chat(
        model=ROUTER_MODEL,
        messages=[{"role": "system", "content": prompt}] + messages,
        options=ollama_options,
    )

    rewritten = resp['message']['content'].strip()

    # Guard lần 2: nếu nó trả lời → bỏ
    if any(x in rewritten.lower() for x in ["tín chỉ", "là", "bao gồm"]):
        return query

    return rewritten

@router.post("/chat")
async def chat_endpoint(body: ChatRequest):
    last_message = body.messages[-1]
    user_query = last_message.parts[0].text

    print(f"\n=== USER: {user_query} ===")

    intent = detect_intent(user_query)
    print(intent)

    ollama_host = os.getenv("OLLAMA_HOST")
    ollama_options = build_ollama_options()
    client = ollama.AsyncClient(host=ollama_host) if ollama_host else ollama.AsyncClient()

    history = [
        {"role": m.role, "content": m.parts[0].text}
        for m in body.messages[:-1]
    ]
    print("History:", history)

    if intent == "CORRECTION":
        if history:
            rewritten_query = history[-1]["content"]
        else:
            rewritten_query = user_query

    # Rewrite query nếu là RAG
    if intent == "RAG":
        rewritten_query = await rewrite_query(client, history, user_query, ollama_options)
    else:
        rewritten_query = user_query
    print("Rewrite:", rewritten_query)
    if intent == "GREETING":
        async def stream_results():
            yield f'0:{json.dumps("Xin chào! Mình có thể giúp gì cho bạn về chương trình đào tạo CTU?")}\n'

        return StreamingResponse(
            stream_results(),
            media_type="text/plain",
            headers={"X-Vercel-AI-Data-Stream": "v1"}
        )
    
    # BƯỚC 1: GỌI PIPELINE CHÍNH
    try:
        context = await bot.get_context(rewritten_query)
    except Exception as e:
        print("ERROR get_context:", e)
        context = None

    print(f"=== CONTEXT: {context} ===")

    if not context:
        async def stream_results():
            yield f'0:{json.dumps("Mình không tìm thấy thông tin chính xác trong hệ thống.")}\n'

        return StreamingResponse(
            stream_results(),
            media_type="text/plain",
            headers={"X-Vercel-AI-Data-Stream": "v1"}
        )

    # BƯỚC 2: STREAM RESPONSE
    async def stream_results():
        history = [
            {"role": m.role, "content": m.parts[0].text}
            for m in body.messages[:-1]
        ]

        system_prompt = (
            "Bạn là chatbot tư vấn Đại học Cần Thơ (CTU).\n"
            "Chỉ trả lời dựa trên dữ liệu được cung cấp.\n"
            "Hãy trả lời sinh viên một cách thân thiện.\n"
            "Không được tự bịa.\n"
            "Nếu không có dữ liệu → trả lời: 'Mình không tìm thấy thông tin chính xác.'\n"
        )

        # inject context
        if context:
            system_prompt += f"\nDữ liệu:\n{context}\n"

        async for part in await client.chat(
            model=FINAL_MODEL,
            messages=[{"role": "system", "content": system_prompt}]
                     + history
                     + [{"role": "user", "content": rewritten_query}],
            options=ollama_options,
            stream=True,
        ):
            content = part['message']['content']
            # if not context:
            #     context = "Không tìm thấy dữ liệu phù hợp trong hệ thống."
            if content:
                yield f'0:{json.dumps(content)}\n'
                print(f"Trả lời: {content}")

    return StreamingResponse(
        stream_results(),
        media_type="text/plain",
        headers={"X-Vercel-AI-Data-Stream": "v1"}
    )
