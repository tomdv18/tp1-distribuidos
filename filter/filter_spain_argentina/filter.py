from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher

END = "EOF"
SEPARATOR = "-*-"
queue_manager_input = QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='gateway_metadata', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()  
queue_manager_output.declare_exchange('filter_spain_argentina', 'direct')

binds = [str(i) for i in range(10)] + ["-1"]

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='gateway_metadata', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(_ch, method, _properties, body):
    if method.routing_key == "-1" and body.decode() == END:  # La segunda de las condiciones puede ser redundante
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager_input.stop_consuming()
        queue_manager_input.close_connection()
        queue_manager_output.publish_message(exchange_name='filter_spain_argentina', routing_key="-1", message=END)
        queue_manager_output.close_connection()
        return
    
    body_split = body.decode().split(SEPARATOR)
    if "spain" in body_split[4].lower() and "argentina" in body_split[4].lower():
        movie_id = body_split[0]
        genres = body_split[1]
        title = body_split[7]
        release_date = body_split[5]
        row_str = f"{movie_id}{SEPARATOR}{title}{SEPARATOR}{genres}{SEPARATOR}{release_date}"
        queue_manager_output.publish_message(
            exchange_name='filter_spain_argentina', routing_key=str(movie_id[-1]), message=row_str)
        




queue_manager_input.consume_messages(
    queue_name=queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
