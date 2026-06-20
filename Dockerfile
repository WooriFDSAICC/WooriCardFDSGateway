FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN groupadd --system woori && useradd --system --gid woori woori

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/
COPY run.py .

RUN chown -R woori:woori /app
USER woori

EXPOSE 8010

CMD ["uvicorn", "run:app", "--host", "0.0.0.0", "--port", "8010", "--workers", "1"]
