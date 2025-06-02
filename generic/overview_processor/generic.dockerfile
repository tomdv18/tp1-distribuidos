FROM base_nlp:0.0.1
RUN pip install huggingface_hub[hf_xet]
COPY generic/generic.py /app/generic.py
COPY generic/overview_processor/overview_processor.py /app/overview_processor.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/ /app/
WORKDIR /app
CMD ["python3", "/app/overview_processor.py"]