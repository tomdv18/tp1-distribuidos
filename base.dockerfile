FROM ubuntu:25.04

RUN apt update && apt install -y python3 python3-pip python3-venv

RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

RUN pip install pika