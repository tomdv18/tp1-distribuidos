FROM base:0.0.1

COPY join/join_ratings/join.py /app/join.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py

WORKDIR /app
CMD ["python3", "/app/join.py"]