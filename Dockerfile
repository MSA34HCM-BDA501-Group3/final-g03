FROM node:22-alpine AS ui-builder

WORKDIR /build/toc_frontend
COPY toc_frontend/package.json toc_frontend/package-lock.json* ./
RUN npm ci --no-audit --no-fund || npm install --no-audit --no-fund
COPY toc_frontend/ ./
RUN npm run build


FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY baseline.py pipeline.py producer.py starter_stream.py live_kafka_stream.py spark_engine.py spark_batch.py ./
COPY scripts ./scripts
COPY traffic_data ./traffic_data
COPY toc_backend ./toc_backend
COPY --from=ui-builder /build/toc_frontend/dist ./toc_frontend/dist
COPY data/demo_sample ./data/demo_sample
COPY evidence ./evidence
COPY product ./product
RUN mkdir -p /app/raw_data /app/data /app/outputs

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')"

CMD ["uvicorn", "toc_backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
