FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8003

CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8003"]