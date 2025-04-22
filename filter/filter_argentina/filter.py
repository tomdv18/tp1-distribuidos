from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants
import os

queue_manager_input = QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='gateway_metadata', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()  
queue_manager_output.declare_exchange('filter_argentina', 'direct')

binds_env = os.getenv("BINDS", "")
binds = (binds_env.split(",") if binds_env else [])
ended = 0

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='gateway_metadata', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(_ch, method, _properties, body):
    if body.decode() == constants.END:
        queue_manager_output.publish_message(exchange_name='filter_argentina', routing_key=method.routing_key, message=constants.END)
        print(f" [*] Received EOF for bind {method.routing_key}")
        global ended
        ended += 1
        if ended == len(binds):
            print(" [*] Received EOF for all movies, exiting...")
            queue_manager_input.stop_consuming()
            queue_manager_input.close_connection()
            queue_manager_output.close_connection()
            return
    else:
        body_split = body.decode().split(constants.SEPARATOR)
        if "argentina" in body_split[4].lower():
            movie_id = body_split[0]
            release_date = body_split[5]
            title = body_split[1]
            row_str = f"{movie_id}{constants.SEPARATOR}{release_date}{constants.SEPARATOR}{title}"
            queue_manager_output.publish_message(
                exchange_name='filter_argentina', routing_key=str(movie_id[-1]), message=row_str)
        




queue_manager_input.consume_messages(
    queue_name=queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
