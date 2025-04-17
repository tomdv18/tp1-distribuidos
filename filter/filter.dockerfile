FROM base:0.0.1

COPY filter.py /app/filter.py
WORKDIR /app
CMD ["python3", "/app/filter.py"]