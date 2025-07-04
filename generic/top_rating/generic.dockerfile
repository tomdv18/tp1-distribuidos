FROM base:0.0.1

COPY generic/generic.py /app/generic.py
COPY generic/top_rating/top_rating.py /app/top_rating.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/ /app/
WORKDIR /app
CMD ["python3", "/app/top_rating.py"]