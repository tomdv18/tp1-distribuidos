from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants
import os

queue_manager_input_metadata = QueueManagerConsumer()
queue_manager_input_metadata.declare_exchange(exchange_name='filter_years_2000_argentina', exchange_type='direct')
queue_name_metadata = queue_manager_input_metadata.queue_declare(queue_name='')

queue_manager_input_ratings = QueueManagerConsumer()
queue_manager_input_ratings.declare_exchange(exchange_name='gateway_ratings', exchange_type='direct')
queue_name_ratings = queue_manager_input_ratings.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()  
queue_manager_output.declare_exchange('join_ratings', 'direct')

binds_env = os.getenv("BINDS", "")
binds = (binds_env.split(",") if binds_env else [])
ended_metadata = 0
ended_ratings = 0

for bind in binds:
    queue_manager_input_metadata.queue_bind(
        exchange_name='filter_years_2000_argentina', queue_name=queue_name_metadata, routing_key=bind)
    queue_manager_input_ratings.queue_bind(
        exchange_name='gateway_ratings', queue_name=queue_name_ratings, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

titles = {}
done = 0
results = {}

def callback_metadata(_ch, method, _properties, body):
    if body.decode() == constants.END:  
        print(f" [*] Received EOF for metadata bind {method.routing_key}")
        global ended_metadata
        ended_metadata += 1
        if ended_metadata == len(binds):
            queue_manager_input_metadata.stop_consuming()
            queue_manager_input_metadata.close_connection()
            global done
            done += 1
            if done == 2:
                send_pending_titles()
            return
    else:
        body_split = body.decode().split(constants.SEPARATOR)
        movie_id = body_split[0]
        title = body_split[1]
        if movie_id not in titles:
            titles[movie_id] = title

def callback_ratings(_ch, method, _properties, body):
    if body.decode() == constants.END: 
        print(f" [*] Received EOF for ratings bind {method.routing_key}")
        global ended_ratings
        ended_ratings += 1
        if ended_ratings == len(binds):
            queue_manager_input_ratings.stop_consuming()
            queue_manager_input_ratings.close_connection()
            global done
            done += 1
            if done == 2:
                send_pending_titles()
            return
    else:
        body_split = body.decode().split(constants.SEPARATOR)
        movie_id = body_split[0]
        rating = body_split[1]
        if movie_id not in titles:
            results[movie_id] = rating
        else:
            title = titles[movie_id]
            row_str = f"{movie_id}{constants.SEPARATOR}{rating}{constants.SEPARATOR}{title}"
            queue_manager_output.publish_message(
                exchange_name='join_ratings', routing_key=str(movie_id[-1]), message=row_str)

     
def send_pending_titles():
    for id, rating in results.items():
        if id not in titles:
            print(f" [*] Movie ID {id} not found in titles (metadata), skipping...")
            continue
        title = titles[id]
        row_str = f"{id}{constants.SEPARATOR}{rating}{constants.SEPARATOR}{title}"
        queue_manager_output.publish_message(exchange_name='join_ratings', routing_key=id[-1], message=row_str)
    for bind in binds:
        queue_manager_output.publish_message(exchange_name='join_ratings', routing_key=bind, message=constants.END)
    queue_manager_output.close_connection()
        


queue_manager_input_metadata.consume_messages(
    queue_name=queue_name_metadata,
    callback=callback_metadata
)
queue_manager_input_metadata.start_consuming()

queue_manager_input_ratings.consume_messages(
    queue_name=queue_name_ratings,
    callback=callback_ratings
)
queue_manager_input_ratings.start_consuming()
