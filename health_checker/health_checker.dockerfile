FROM ubuntu:25.04

RUN apt update && apt install -y python3 python3-pip python3-venv docker.io

RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

COPY health_checker/health_checker.py /app/health_checker.py
WORKDIR /app
CMD ["python", "/app/health_checker.py"]