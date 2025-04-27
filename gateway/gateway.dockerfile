FROM base:0.0.1

COPY gateway/gateway.py /app/gateway.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
WORKDIR /app
EXPOSE 5050
CMD ["python3", "/app/gateway.py"]