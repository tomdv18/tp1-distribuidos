FROM base:0.0.1

COPY filter/filter_spain_argentina/filter.py /app/filter.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
WORKDIR /app
CMD ["python3", "/app/filter.py"]