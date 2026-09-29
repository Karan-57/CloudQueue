FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p database

EXPOSE 8080

CMD ["sh", "-c", "python -c \"from app import init_db; init_db()\" && gunicorn --bind 0.0.0.0:${PORT:-8080} app:app"]