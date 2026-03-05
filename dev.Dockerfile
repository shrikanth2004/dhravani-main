# ---- Stage 1: Build Python environment ----
FROM python:3.11-slim AS builder

WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv /opt/venv

# Set up Python environment
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download PocketBase and extract it
RUN wget -O pocketbase.zip https://github.com/pocketbase/pocketbase/releases/download/v0.25.0/pocketbase_0.25.0_linux_amd64.zip \
    && unzip pocketbase.zip -d /app/ \
    && rm pocketbase.zip \
    && chmod +x /app/pocketbase

# ---- Stage 2: Final container ----
FROM python:3.11-slim

WORKDIR /app

# Install necessary system dependencies for Flask & PocketBase
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* 

# Copy Python virtual environment
COPY --from=builder /opt/venv /opt/venv

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH" \
    FLASK_APP=app.py \
    PYTHONUNBUFFERED=1

# Set up PocketBase directory structure first
RUN mkdir -p /app/pocketbase \
    && mkdir -p /app/pocketbase/pb_public \
    && mkdir -p /app/pocketbase/pb_data

# Copy application files
COPY . .

# Copy and setup PocketBase binary
COPY --from=builder /app/pocketbase /usr/local/bin/pocketbase
RUN chmod +x /usr/local/bin/pocketbase \
    && chmod -R 755 /app/pocketbase \
    && ls -la /usr/local/bin/pocketbase

# Expose Flask (5000) and PocketBase (8090) ports
EXPOSE 5000 8090

# Create an entrypoint script to start both services
RUN echo '#!/bin/sh\n\
    echo "Current directory: $(pwd)"\n\
    echo "Checking PocketBase binary:"\n\
    which pocketbase\n\
    cd /app/pocketbase && pocketbase serve --http="0.0.0.0:8090" &\n\
    python3 /app/app.py' > /app/start.sh \
    && chmod +x /app/start.sh

# Set default command
CMD ["/bin/sh", "/app/start.sh"]
