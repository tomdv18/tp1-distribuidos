from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants
import os

queue_manager_input= QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='group_by_movie', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()
queue_manager_output.declare_exchange('results', 'direct')

top_rating = None
worst_rating = None

binds_env = os.getenv("BINDS", "")
binds = (binds_env.split(",") if binds_env else [])
ended = 0

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='group_by_movie', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

def callback(_ch, method, _properties, body):
    global top_rating
    global worst_rating
    if body.decode() == constants.END:
        print(f" [*] Received EOF for bind {method.routing_key}")
        global ended
        ended += 1
        if ended == len(binds):
            print(" [*] Received EOF for all movies, exiting...")
            queue_manager_input.stop_consuming()
            queue_manager_input.close_connection()
            
            row_str = f"Query 3 -> {top_rating[0]} {top_rating[1]} {top_rating[2]}"
            queue_manager_output.publish_message(exchange_name='results', routing_key='results', message=row_str)
            row_str = f"Query 3 -> {worst_rating[0]} {worst_rating[1]} {worst_rating[2]}"
            queue_manager_output.publish_message(exchange_name='results', routing_key='results', message=row_str)
            
            queue_manager_output.publish_message(exchange_name='results', routing_key='results', message=constants.END)
            queue_manager_output.close_connection()
            return
    else:
        body_split = body.decode().split(constants.SEPARATOR)
        movie_id = body_split[0]
        title = body_split[1]
        rating = float(body_split[2])
    
        if top_rating is None or rating > top_rating[2]:
            top_rating = (movie_id, title, rating)
        if worst_rating is None or rating < worst_rating[2]:
            worst_rating = (movie_id, title, rating)
        

queue_manager_input.consume_messages(
    queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
