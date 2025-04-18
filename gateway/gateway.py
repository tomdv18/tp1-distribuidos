#!/usr/bin/env python
import csv
from queue_manager.queue_manager import QueueManagerPublisher

movies = []

with open('movies_metadata.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        movie_id = row[5] 
        row_str = ",".join(row)
        movies.append((movie_id, row_str))

queue_manager = QueueManagerPublisher()  
queue_manager.declare_exchange('gateway_metadata', 'direct')

for movie in movies:
    movie_id, row = movie
    queue_manager.publish_message(
        exchange_name='gateway_metadata', routing_key=movie_id[-1], message=row)
    print(f" [x] Sending {movie_id} with key {movie_id[-1]}")

queue_manager.close_connection()