FROM python:3.11-slim

# poppler-utils: utilidades de PDF disponibles para los parsers del backend
RUN apt-get update \
    && apt-get install -y --no-install-recommends poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# 1 worker: el flujo preview→confirmar usa un cache en memoria del proceso (app/services/cache.py) — con >1 worker el confirmar puede caer en otro proceso y fallar con "sesión expirada". Escalar workers recién si ese cache sale del proceso (ej. Redis).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
