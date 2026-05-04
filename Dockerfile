FROM python:3.11-slim

LABEL maintainer="Ricardo Macedo Martins"
LABEL description="Azure Connection Resilience Monitor — node-agent daemon"

WORKDIR /app

# Install only what we need
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY monitor.py .
COPY couchbase_adapter.py .
COPY config.yaml .

# Expose Prometheus metrics port (loopback by default, override with --metrics-bind)
EXPOSE 9090

# Health check via metrics endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9090/metrics')" || exit 1

# Run monitor with production defaults:
#   - 1s polling (per Microsoft guidance)
#   - Notify on loopback (adapter must be on same host)
#   - Auth token from environment variable
ENTRYPOINT ["python", "monitor.py"]
CMD ["--notify-url", "http://127.0.0.1:8099/drain", "--poll-interval", "1"]
