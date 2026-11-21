FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY demo ./demo

# FastAPI query endpoint
EXPOSE 8000
# UDP ingress ports A/B and the forward port are declared for documentation;
# docker-compose maps them explicitly.
EXPOSE 9100/udp 9101/udp 9200/udp

CMD ["python", "-m", "app.main"]
