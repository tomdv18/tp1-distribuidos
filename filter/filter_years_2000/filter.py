from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants


queue_manager_input = QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='filter_spain_argentina', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()
queue_manager_output.queue_declare(queue_name='results', exclusive=False)

binds = [str(i) for i in range(10)] + ["-1"]

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='filter_spain_argentina', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(_ch, method, _properties, body):
    if method.routing_key == "-1" and body.decode() == constants.END:  # La segunda de las condiciones puede ser redundante
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager_input.stop_consuming()
        queue_manager_input.close_connection()
        queue_manager_output.publish_message(exchange_name='', routing_key='results', message=constants.END)
        queue_manager_output.close_connection()
        return
    
    body_split = body.decode().split(constants.SEPARATOR)
    realease_date = body_split[3]
    print(f" [x] Received {body.decode()}")
    if int(realease_date.split("-")[0]) >= 2000:
        movie_id = body_split[0]
        genres = body_split[2]
        title = body_split[1]
        row_str = f"Query 1 -> ID: {movie_id} - Title: {title} - Genres: {genres}"
        queue_manager_output.publish_message(
            exchange_name='', routing_key='results', message=row_str)


                 
queue_manager_input.consume_messages(
    queue_name=queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
