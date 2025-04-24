FROM base:0.0.1
RUN pip install pika transformers torch

COPY model_downloader.py /app/model_downloader.py

WORKDIR /app

CMD ["python3", "model_downloader.py"]