from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants


queue_manager_input = QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='gateway_metadata', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()  
queue_manager_output.declare_exchange('filter_one_prod', 'direct')

binds = [str(i) for i in range(10)] + ["-1"]

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='gateway_metadata', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(_ch, method, _properties, body):
    if method.routing_key == "-1" and body.decode() == constants.END:  # La segunda de las condiciones puede ser redundante
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager_input.stop_consuming()
        queue_manager_input.close_connection()
        queue_manager_output.publish_message(exchange_name='filter_one_prod', routing_key="-1", message=constants.END)
        queue_manager_output.close_connection()
        return
    
    body_split = body.decode().split(constants.SEPARATOR)
    if body_split[4].count(',') == 1:
        try:
            country_name = body_split[4].split("'name': '")[1].split("'")[0]
        except IndexError:
            country_name = body_split[4].split("'name': \"")[1].split('"')[0]
            

        movie_id = body_split[0]
        budget = body_split[2]
        row_str = f"{country_name}{constants.SEPARATOR}{budget}"
        queue_manager_output.publish_message(exchange_name='filter_one_prod', routing_key=str(movie_id[-1]), message=row_str)
        


queue_manager_input.consume_messages(
    queue_name=queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
