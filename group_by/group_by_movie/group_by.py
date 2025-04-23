from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants
import os

queue_manager_input = QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='join_ratings', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()
queue_manager_output.declare_exchange('group_by_movie', 'direct')

binds_env = os.getenv("BINDS", "")
binds = (binds_env.split(",") if binds_env else [])
ended = 0

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='join_ratings', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

results = {}

def callback(_ch, method, _properties, body):
    if body.decode() == constants.END:
        print(f" [*] Received EOF for bind {method.routing_key}")
        global ended
        ended += 1
        if ended == len(binds):
            queue_manager_input.stop_consuming()
            queue_manager_input.close_connection()
            for key, (total_rating, count) in results.items():
                row_str = f"{key}{constants.SEPARATOR}{total_rating / count}"
                queue_manager_output.publish_message(exchange_name='group_by_movie', routing_key=key.split(constants.SEPARATOR)[0][-1], message=row_str)
            for bind in binds:
                queue_manager_output.publish_message(exchange_name='group_by_movie', routing_key=bind, message=constants.END)
            queue_manager_output.close_connection()
            return
    else:
        body_split = body.decode().split(constants.SEPARATOR)
        movie_id = body_split[0]
        title = body_split[2]
        key = movie_id + constants.SEPARATOR + title
        rating = float(body_split[1])
        if key not in results:
            results[key] = (rating, 1)
        else:
            current_rating, count = results[key]
            results[key] = (current_rating + rating, count + 1)

        

queue_manager_input.consume_messages(
    queue_name=queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
