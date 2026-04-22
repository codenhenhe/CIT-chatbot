import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from app.api import auth, chat, graph
from app.services.retrieval_service import warmup_embedding_model
from app.services.llm_service import warmup_llm_model, unload_llm_models


def _setup_logging() -> None:
    level_name = os.getenv("APP_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )
    for logger_name in ("app", "app.chat", "app.retrieval", "app.llm", "app.domain", "app.entity", "app.intent"):
        logging.getLogger(logger_name).setLevel(level)


_setup_logging()


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
    try:
        warmed_up = await asyncio.to_thread(warmup_embedding_model)
        if warmed_up:
            print("[Startup] BGE embedding model warmed up")
        else:
            print("[Startup] Embedding warmup skipped due to initialization error")
    except Exception as e:
        print(f"[Startup] Embedding warmup skipped: {e}")

    # Warmup LLM model (9b)
    try:
        llm_warmed_up = await warmup_llm_model()
        if llm_warmed_up:
            print("[Startup] Ollama LLM model (9b) warmed up")
        else:
            print("[Startup] LLM warmup incomplete but continuing")
    except Exception as e:
        print(f"[Startup] LLM warmup error: {e}")

    await graph.start_ingestion_worker()


@app.on_event("shutdown")
async def shutdown_event():
    await graph.stop_ingestion_worker()
    
    # Unload LLM models gracefully
    try:
        await unload_llm_models()
        print("[Shutdown] LLM models unload signals sent")
    except Exception as e:
        print(f"[Shutdown] Error during LLM unload: {e}")

@app.get("/")
def root():
    return {"msg": "API running"}







