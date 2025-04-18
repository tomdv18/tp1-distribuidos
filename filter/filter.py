import sys
from queue_manager.queue_manager import QueueManagerConsumer

END = "EOF"


queue_manager = QueueManagerConsumer()

queue_manager.declare_exchange(exchange_name='gateway_metadata', exchange_type='direct')
queue_name = queue_manager.queue_declare(queue_name='')

binds = ["1" , "3", "6", "9", "-1"]

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
    body_split = body.decode().split("--")
    if "spain" in body_split[3].lower() and "argentina" in body_split[3].lower():
        print(f" [x] Pelicula con produccion argentina y española: {body_split[0]} - {body_split[5]}")
        # Aquí puedes agregar la lógica para filtrar y procesar los mensajes que cumplan con tus criterios




queue_manager.consume_messages(
    queue_name=queue_name,
    callback=callback
)

queue_manager.start_consuming()
