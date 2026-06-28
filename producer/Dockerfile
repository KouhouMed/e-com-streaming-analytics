FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .

# Ensure print() output appears immediately in docker logs
ENV PYTHONUNBUFFERED=1

CMD ["python", "generator.py"]
