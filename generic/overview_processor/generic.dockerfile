FROM base:0.0.1
RUN pip install pika transformers torch huggingface_hub[hf_xet]
COPY generic/generic.py /app/eneric.py
COPY generic/overview_processor/overview_processor.py /app/overview_processor.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/node.py /app/node.py
WORKDIR /app
CMD ["python3", "/app/overview_processor.py"]