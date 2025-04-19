FROM base:0.0.1

COPY top/top_budget/top.py /app/top.py
COPY queue_manager /app/queue_manager
WORKDIR /app
CMD ["python3", "/app/top.py"]