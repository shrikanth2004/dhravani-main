# Combined Dockerfile with Flask + PocketBase + PostgreSQL (All in One)
# Build: docker build -f dev.Dockerfile -t dhravani .
# Run: docker run -p 5000:5000 -p 8090:8090 dhravani

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

# Install PostgreSQL and necessary system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql \
    postgresql-contrib \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Configure PostgreSQL
RUN mkdir -p /var/run/postgresql && \
    chown -R postgres:postgres /var/run/postgresql && \
    chown -R postgres:postgres /var/log/postgresql

# Create PostgreSQL data directory
RUN mkdir -p /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql

# Copy Python virtual environment
COPY --from=builder /opt/venv /opt/venv

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH" \
    FLASK_APP=app.py \
    PYTHONUNBUFFERED=1 \
    PGUSER=postgres \
    PGDATA=/var/lib/postgresql/data \
    POSTGRES_URL="postgresql://postgres@localhost:5432/postgres"

# Set up PocketBase directory structure first
RUN mkdir -p /app/pocketbase \
    && mkdir -p /app/pocketbase/pb_public \
    && mkdir -p /app/pocketbase/pb_data

# Copy application files
COPY . .

# Copy and setup PocketBase binary
COPY --from=builder /app/pocketbase /usr/local/bin/pocketbase
RUN chmod +x /usr/local/bin/pocketbase \
    && chmod -R 755 /app/pocketbase

# Create datasets directory
RUN mkdir -p /app/datasets && chmod 777 /app/datasets

# Expose Flask (5000) and PocketBase (8090) ports
EXPOSE 5000 8090

# Create an entrypoint script to start all services
RUN echo '#!/bin/sh \
    echo "=========================================" && \
    echo "Starting PostgreSQL..." && \
    # Initialize PostgreSQL if not done \
    if [ ! -d "/var/lib/postgresql/data/base" ]; then \
        su - postgres -c "initdb -D /var/lib/postgresql/data"; \
    fi && \
    # Start PostgreSQL in background \
    su - postgres -c "pg_ctl -D /var/lib/postgresql/data -l /var/log/postgresql/logfile start" && \
    # Wait for PostgreSQL to be ready \
    for i in $(seq 1 30); do \
        su - postgres -c "pg_isready" && break; \
        echo "Waiting for PostgreSQL..."; \
        sleep 1; \
    done && \
    echo "PostgreSQL started!" && \
    echo "=========================================" && \
    echo "Starting PocketBase..." && \
    cd /app/pocketbase && pocketbase serve --http="0.0.0.0:8090" & \
    echo "PocketBase started!" && \
    echo "=========================================" && \
    echo "Starting Flask Application..." && \
    cd /app && python3 app.py' > /app/start.sh

RUN chmod +x /app/start.sh

# Set default command
CMD ["/bin/sh", "/app/start.sh"]

