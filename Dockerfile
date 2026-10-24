FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    DATABASE_URL=sqlite:////tmp/de_agent.db
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Run as a non-root user
RUN useradd --create-home appuser
USER appuser

EXPOSE 8000
# Render injects $PORT; default to 8000 locally
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
