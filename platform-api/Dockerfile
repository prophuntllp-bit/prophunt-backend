FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY platform-api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY platform-api/. .
EXPOSE 8013
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8013"]
