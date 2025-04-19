#!/usr/bin/env python
import csv
from queue_manager.queue_manager import QueueManagerPublisher, QueueManagerConsumer
import constants


queue_manager_metadata = QueueManagerPublisher()  
queue_manager_metadata.declare_exchange('gateway_metadata', 'direct')

queue_manager_ratings = QueueManagerPublisher()  
queue_manager_ratings.declare_exchange('gateway_ratings', 'direct')

queue_manager_results = QueueManagerConsumer()
queue_manager_results.queue_declare(queue_name='results', exclusive=False)

eof_count = 0
EOF_WAITING = 1

with open('movies_metadata.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        if len(row) < 24:  
            continue


        movie_id = row[5] 
        budget = row[2]
        genres = row[3]
        overview = row[9]
        production_countries = row[13]
        realease_date = row[14]
        revenue = row[15]
        title = row[20]
        if not movie_id or not budget or not genres or not overview or not production_countries or not realease_date or not revenue or not title:
            continue
        
 
        row_str = f"{movie_id}{constants.SEPARATOR}{genres}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{overview}{constants.SEPARATOR}{production_countries}{constants.SEPARATOR}{realease_date}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{title}"
        queue_manager_metadata.publish_message(
        exchange_name='gateway_metadata', routing_key=str(movie_id[-1]), message=row_str)
        print(f" [METADATA] Sending {movie_id} with key {movie_id[-1]}")

print(" [x] Sending EOF message from metadata")
queue_manager_metadata.publish_message(
    exchange_name='gateway_metadata', routing_key="-1", message=constants.END)
queue_manager_metadata.close_connection()


with open('ratings.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        if len(row) < 4:  
            continue


        movie_id = row[1] 
        rating = row[2]

        if not movie_id or not rating:
            continue
        
        row_str = f"{movie_id}{constants.SEPARATOR}{rating}"
        queue_manager_ratings.publish_message(
        exchange_name='gateway_ratings', routing_key=str(movie_id[-1]), message=row_str)
        print(f" [RATINGS] Sending {movie_id} with key {movie_id[-1]}")

print(" [x] Sending EOF message from ratings")
queue_manager_ratings.publish_message(
    exchange_name='gateway_ratings', routing_key="-1", message=constants.END)
queue_manager_ratings.close_connection()

def callback(ch, method, properties, body):
    global eof_count
    if body.decode() == constants.END:
        print(f" [*] Received {body.decode()} for all movies, exiting...")
        eof_count += 1
        if eof_count == EOF_WAITING:
            queue_manager_results.stop_consuming()
            queue_manager_results.close_connection()
            return
    
    print(f" [x] Received {body.decode()}")

queue_manager_results.consume_messages(
    queue_name='results',
    callback=callback
)
queue_manager_results.start_consuming()