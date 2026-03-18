from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from app.scripts.prototype import GraphRAGChatbot
import json
from pydantic import BaseModel
from typing import List, Optional
import ollama

app = FastAPI(title="CTU GraphRAG Assistant API")

# Giữ nguyên cấu trúc CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bot = GraphRAGChatbot()

# 1. Định nghĩa Model khớp hoàn toàn với dữ liệu "parts" bạn đã debug
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

ROUTER_MODEL = "qwen2.5-coder:1.5b"  # Model nhỏ - xác định intent
FINAL_MODEL = "qwen2.5-coder:3b-instruct" # Model chính để trả lời

@app.post("/api/chat")
async def chat_endpoint(body: ChatRequest):
    last_message = body.messages[-1]
    user_query = last_message.parts[0].text
    
    # BƯỚC 1: ROUTER SIÊU ĐƠN GIẢN (Chỉ lấy nhãn)
    # Ta không yêu cầu nó 'rewritten_query' nữa để tránh nó nhét rác vào database
    router_prompt = (
        "Bạn là bộ điều phối chatbot cho Đại học Cần Thơ. "
        "Nhiệm vụ: Phân loại câu hỏi của người dùng. "
        "1. Nếu là chào hỏi, cảm ơn, hỏi thăm: Trả về 'GREETING'. "
        "2. Nếu hỏi về môn học, mã học phần, tín chỉ, chương trình đào tạo: Trả về 'RAG'. "
        "Chỉ trả về đúng 1 từ duy nhất."
    )
    
    client = ollama.AsyncClient()
    try:
        # Gọi model 1.5b nhanh gọn
        route_resp = await client.generate(model=ROUTER_MODEL, prompt=user_query, system=router_prompt)
        intent = route_resp['response'].strip().upper()
    except:
        intent = "RAG" # Mặc định là tra cứu nếu lỗi

    # BƯỚC 2: TRUY VẤN DỮ LIỆU
    context = ""
    if "RAG" in intent:
        # Linh hãy dùng trực tiếp user_query ở đây để đảm bảo tính nguyên bản
        print(f"--- [RAG Mode] Đang truy vấn cho: {user_query} ---")
        context = await bot.layer_2_hybrid_retrieval(user_query) or \
                  await bot.layer_1_semantic_search(user_query) or \
                  await bot.layer_3_graph_reasoning(user_query)
    else:
        print(f"--- [Direct Mode] Phản hồi trực tiếp ---")

    # BƯỚC 3: TỔNG HỢP (Hàng rào bảo vệ cho CTU)
    async def stream_results():
        # Lịch sử chat
        history = [{"role": m.role, "content": m.parts[0].text} for m in body.messages[:-1]]

        system_prompt = (
            "Bạn là chuyên viên tư vấn đào tạo của Đại học Cần Thơ (CTU). "
            "Nhiệm vụ: Dựa vào ngữ cảnh trích xuất từ đồ thị để hỗ trợ sinh viên. "
            "Nếu ngữ cảnh rỗng, hãy khuyên sinh viên liên hệ phòng đào tạo hoặc văn phòng khoa."
        )
        if context:
            system_prompt += f"\nNgữ cảnh đồ thị: {context}"
        print(f"Context: {context}")  
        async for part in await client.chat(
            model=FINAL_MODEL,
            messages=[{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": user_query}],
            stream=True,
        ):
            content = part['message']['content']
            if content:
                yield f'0:{json.dumps(content)}\n' 
                
    return StreamingResponse(stream_results(), media_type="text/plain", headers={"X-Vercel-AI-Data-Stream": "v1"})

# @app.post("/api/chat")
# async def chat_endpoint(body: ChatRequest):
#     # 2. Trích xuất nội dung tin nhắn cuối cùng
#     last_message = body.messages[-1]
#     user_query = last_message.parts[0].text
    
#     # 3. Chạy logic GraphRAG
#     context = await bot.layer_2_hybrid_retrieval(user_query) or \
#               await bot.layer_1_semantic_search(user_query) or \
#               await bot.layer_3_graph_reasoning(user_query)
    
#     if not context:
#         context = "Không tìm thấy dữ liệu cụ thể trong hệ thống."

#     # 4. Trả về luồng StreamingResponse thay vì JSON tĩnh
#     async def stream_results():
#         # Chuẩn bị lịch sử chat cho Ollama
#         history = []
#         for m in body.messages[:-1]:
#             history.append({"role": m.role, "content": m.parts[0].text})

#         system_prompt = (
#             "Bạn là chuyên viên tư vấn tuyển sinh và đào tạo của Đại học Cần Thơ (CTU). "
#             "Nhiệm vụ của bạn là hỗ trợ sinh viên dựa trên ngữ cảnh được cung cấp. "
#             f"Ngữ cảnh trích xuất từ đồ thị: {context}"
#         )
#         print(f"DEBUG - Ngữ cảnh sử dụng: {context[:100]}...")
        
#         client = ollama.AsyncClient()
#         async for part in await client.chat(
#             model=bot.llm_model,
#             messages=[{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": user_query}],
#             stream=True,
#         ):
#             content = part['message']['content']
#             if content:
#                 print(content, end="", flush=True)
#                 yield f'0:{json.dumps(content)}\n' 
                
#         print("\nDEBUG - Hoàn thành luồng stream.")
#     return StreamingResponse(stream_results(), media_type="text/plain", headers={"X-Vercel-AI-Data-Stream": "v1"})
    # return StreamingResponse(stream_results(), media_type="text/event-stream")