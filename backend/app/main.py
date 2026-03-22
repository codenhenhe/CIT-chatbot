from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, chat, graph


app = FastAPI(title="CTU GraphRAG Assistant API")

app.include_router(auth.router, prefix="/admin")
app.include_router(chat.router)
app.include_router(graph.router, prefix="/graph")

# Giữ nguyên cấu trúc CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    await graph.start_ingestion_worker()


@app.on_event("shutdown")
async def shutdown_event():
    await graph.stop_ingestion_worker()

@app.get("/")
def root():
    return {"msg": "API running"}







