# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt requirements-dev.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY .env.example ./

# Create logs directory
RUN mkdir -p logs

# Set Python path
ENV PYTHONPATH=/app/src

# Expose port for Cloud Run
EXPOSE 8080

# Create startup script
COPY docker/start.sh ./
RUN chmod +x start.sh

# Run the MCP server
CMD ["./start.sh"]
