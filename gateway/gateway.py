#!/usr/bin/env python
import csv
from queue_manager.queue_manager import QueueManagerPublisher, QueueManagerConsumer

END = "EOF"
SEPARATOR = "-*-"
queue_manager_metadata = QueueManagerPublisher()  
queue_manager_metadata.declare_exchange('gateway_metadata', 'direct')

queue_manager_results = QueueManagerConsumer()
queue_manager_results.queue_declare(queue_name='results', exclusive=False)


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
        
 
        row_str = f"{movie_id}{SEPARATOR}{genres}{SEPARATOR}{budget}{SEPARATOR}{overview}{SEPARATOR}{production_countries}{SEPARATOR}{realease_date}{SEPARATOR}{revenue}{SEPARATOR}{title}"
        queue_manager_metadata.publish_message(
        exchange_name='gateway_metadata', routing_key=str(movie_id[-1]), message=row_str)
        print(f" [x] Sending {movie_id} with key {movie_id[-1]}")


print(" [x] Sending EOF message")
queue_manager_metadata.publish_message(
    exchange_name='gateway_metadata', routing_key="-1", message=END)
queue_manager_metadata.close_connection()

def callback(ch, method, properties, body):
    if body.decode() == END:
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager_results.stop_consuming()
        queue_manager_results.close_connection()
        return
    
    print(f" [x] Received {body.decode()}")

queue_manager_results.consume_messages(
    queue_name='results',
    callback=callback
)
queue_manager_results.start_consuming()