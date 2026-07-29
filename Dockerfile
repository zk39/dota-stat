FROM python:3.11-slim

WORKDIR /app

COPY . .

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["python3", "server.py"]
