import sys
from queue_manager.queue_manager import QueueManagerConsumer


queue_manager = QueueManagerConsumer()

queue_manager.declare_exchange(exchange_name='gateway_metadata', exchange_type='direct')
queue_name = queue_manager.queue_declare(queue_name='')

binds = ["3"]

for bind in binds:
    queue_manager.queue_bind(
        exchange_name='gateway_metadata', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}:{body}: {properties.headers}")


queue_manager.consume_messages(
    queue_name=queue_name,
    callback=callback
)

queue_manager.start_consuming()