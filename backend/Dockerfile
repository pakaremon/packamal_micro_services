FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    g++ \
    git \
    netcat-traditional \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install psycopg2-binary

# Copy application code
COPY . .

# Create necessary directories
# Make results directory writable (777 permissions for maximum compatibility)
RUN mkdir -p static media results && \
    chmod 777 results

# Make entrypoint executable and fix line endings
RUN sed -i 's/\r$//' entrypoint.sh && \
    chmod +x entrypoint.sh || echo "entrypoint.sh not found yet"

# Expose port
EXPOSE 8000

# Run entrypoint
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
