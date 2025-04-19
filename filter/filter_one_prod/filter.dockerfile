FROM base:0.0.1

COPY filter/filter_one_prod/filter.py /app/filter.py
COPY queue_manager /app/queue_manager
WORKDIR /app
CMD ["python3", "/app/filter.py"]