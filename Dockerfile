FROM python:3.11-slim AS builder

WORKDIR /app
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Use a non-root user
FROM python:3.11-slim
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    FLASK_APP=app.py \
    PYTHONUNBUFFERED=1

COPY . .

# Ensure that /app/datasets exists and set correct permissions
RUN mkdir -p /app/datasets \
    && chown -R 1000:1000 /app/datasets \
    && chmod -R 777 /app/datasets

# Create a non-root user
RUN useradd -m appuser && chown -R appuser /app
USER appuser

VOLUME ["/app/datasets"]

EXPOSE 7860

CMD ["python3", "app.py"]