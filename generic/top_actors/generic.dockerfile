FROM base:0.0.1

COPY generic/generic.py /app/generic.py
COPY generic/top_actors/top_actors.py /app/top_actors.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/node.py /app/node.py
WORKDIR /app
CMD ["python3", "/app/top_actors.py"]