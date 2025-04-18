FROM base:0.0.1

COPY gateway/gateway.py /app/gateway.py
COPY queue_manager /app/queue_manager
WORKDIR /app
CMD ["python3", "/app/gateway.py"]