FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alembic.ini alembic.ini
COPY alembic/ ./alembic/
COPY s3_storage_api/ ./s3_storage_api/

# Use ARG for build-time variables

ENV PORT=8000
ENV HOST=0.0.0.0
CMD ["sh", "-c", "alembic upgrade head && uvicorn s3_storage_api.main:app --host $HOST --port $PORT --access-log=False"]
