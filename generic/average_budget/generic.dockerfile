FROM base:0.0.1

COPY generic/generic.py /app/generic.py
COPY generic/average_budget/average_budget.py /app/average_budget.py
COPY queue_manager /app/queue_manager
COPY common/constants.py /app/constants.py
COPY node/ /app/
WORKDIR /app
CMD ["python3", "/app/average_budget.py"]