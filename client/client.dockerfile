FROM base:0.0.1

WORKDIR /app
COPY client/main.py /app/main.py
COPY client/files/ /app/files/
COPY common/constants.py /app/constants.py
EXPOSE 5051


CMD ["python", "/app/main.py"]