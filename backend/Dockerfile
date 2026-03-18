FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/opt/backend \
    HF_HOME=/opt/backend/.cache/huggingface \
    TRANSFORMERS_CACHE=/opt/backend/.cache/huggingface \
    TORCH_HOME=/opt/backend/.cache/torch

WORKDIR /opt/backend

# Keep OS layer minimal while supporting healthcheck and TLS requests.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel

# Install CPU-only Torch to avoid heavy CUDA packages in the image.
RUN python -m pip install --index-url https://download.pytorch.org/whl/cpu torch

# Install only packages required by backend source code.
RUN python -m pip install \
    fastapi \
    "uvicorn[standard]" \
    pydantic \
    python-dotenv \
    ollama \
    neo4j \
    requests \
    pandas \
    pdfplumber \
    pymupdf4llm \
    sentence-transformers \
    llama-index-core \
    llama-index-embeddings-huggingface

# Copy source code after dependencies for better Docker cache reuse.
COPY app ./app

# Non-root runtime user.
RUN addgroup --system app && adduser --system --ingroup app app \
    && mkdir -p /opt/backend/.cache/huggingface /opt/backend/.cache/torch /opt/backend/uploads \
    && chown -R app:app /opt/backend

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/docs > /dev/null || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]