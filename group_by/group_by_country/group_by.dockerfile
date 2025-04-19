FROM base:0.0.1

COPY group_by/group_by_country/group_by.py /app/group_by.py
COPY queue_manager /app/queue_manager
WORKDIR /app
CMD ["python3", "/app/group_by.py"]