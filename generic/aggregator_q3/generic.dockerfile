FROM base:0.0.1

COPY generic/generic.py /app/generic.py
COPY generic/aggregator_q3/aggregator.py /app/aggregator_q3.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/ /app/
WORKDIR /app
CMD ["python3", "/app/aggregator_q3.py"]