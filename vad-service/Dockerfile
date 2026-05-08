FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app:/app/..
COPY vad-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY vad-service/. .
EXPOSE 8001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
