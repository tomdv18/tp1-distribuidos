FROM base:0.0.1

COPY gateway.py /app/gateway.py
WORKDIR /app
CMD ["python3", "/app/gateway.py"]