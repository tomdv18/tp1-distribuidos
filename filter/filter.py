import sys
from queue_manager.queue_manager import QueueManagerConsumer

END = "EOF"


queue_manager = QueueManagerConsumer()

queue_manager.declare_exchange(exchange_name='gateway_metadata', exchange_type='direct')
queue_name = queue_manager.queue_declare(queue_name='')

binds = ["3", "-1"]

for bind in binds:
    queue_manager.queue_bind(
        exchange_name='gateway_metadata', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(ch, method, properties, body):
    if method.routing_key == "-1" and body.decode() == END:  # La segunda de las condiciones puede ser rebundante
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager.stop_consuming()
        queue_manager.close_connection()
        return
    print(f" [x] {method.routing_key}:{body}")

    


queue_manager.consume_messages(
    queue_name=queue_name,
    callback=callback
)

queue_manager.start_consuming()
