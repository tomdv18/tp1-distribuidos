FROM base_nlp:0.0.1

COPY model_downloader.py /app/model_downloader.py

WORKDIR /app

CMD ["python3", "model_downloader.py"]