FROM python:3.13.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg

WORKDIR /app

COPY evidence/session9_rerun/environment.txt ./requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY app.py baseline.py pipeline.py producer.py starter_stream.py spark_engine.py spark_batch.py ./
COPY traffic_online.py ./
COPY scripts ./scripts
COPY traffic_data ./traffic_data
COPY product ./product
RUN mkdir -p /app/raw_data /app/data /app/outputs

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
