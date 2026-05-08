FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY . .
EXPOSE 8013
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8013}"]
