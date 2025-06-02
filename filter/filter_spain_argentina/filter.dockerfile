FROM base:0.0.1

COPY filter/filter.py /app/filter.py
COPY filter/filter_spain_argentina/filter_spain_argentina.py /app/filter_argentina.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/ /app/
WORKDIR /app
CMD ["python3", "/app/filter_argentina.py"]