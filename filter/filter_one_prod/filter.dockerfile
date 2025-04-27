FROM base:0.0.1

COPY filter/filter.py /app/filter.py
COPY filter/filter_one_prod/filter_one_prod.py /app/filter_one_prod.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/node.py /app/node.py
WORKDIR /app
CMD ["python3", "/app/filter_one_prod.py"]