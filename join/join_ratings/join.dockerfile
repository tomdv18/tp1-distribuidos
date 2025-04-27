FROM base:0.0.1

COPY join/join.py /app/join.py
COPY join/join_ratings/join_ratings.py /app/join_ratings.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/node.py /app/node.py
WORKDIR /app
CMD ["python3", "/app/join_ratings.py"]