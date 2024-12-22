# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Create app data directory for settings and database
RUN mkdir -p /app/data

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY rd2tb.py .

# Create volume mount points
VOLUME ["/zurgdata", "/app/data"]

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    ZURGDATA_FOLDER=/zurgdata \
    TORBOX_MAX_CONCURRENT=2

# Document other available environment variables
LABEL org.opencontainers.image.description="TorBox Client Application" \
      org.opencontainers.image.vendor="TorBox" \
      environment.variable.TORBOX_API_TOKEN="Required: Your TorBox API token" \
      environment.variable.ZURGDATA_FOLDER="Optional: Path to zurg data files (default: /zurgdata)" \
      environment.variable.TORBOX_MAX_CONCURRENT="Optional: Max concurrent operations (default: 2)"

# Run the application
ENTRYPOINT ["python", "rd2tb.py"]
