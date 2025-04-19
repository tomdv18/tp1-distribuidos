FROM base:0.0.1

COPY group_by/group_by_country/group_by.py /app/group_by.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py

WORKDIR /app
CMD ["python3", "/app/group_by.py"]