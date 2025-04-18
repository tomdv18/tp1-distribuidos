#!/usr/bin/env python
import csv
from queue_manager.queue_manager import QueueManagerPublisher

END = "EOF"

movies = []


with open('movies_metadata.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        if len(row) < 24:  
            continue

        movie_id = row[5] 
        #row_str = ",".join(row)
        #movies.append((movie_id, row_str)) #No nos interesa la fila entera
        budget = row[2]
        overview = row[9]
        production_countries = row[13]
        revenue = row[15]
        title = row[20] ## Innecesario, solo para debug   
        #ENTIENDO QUE ESTOS SON SOLO LAS COLUMNAS QUE NOS INTERESAN, PERO NO PARA TODAS LAS QUERYS, EN ALGUNAS NO VAN TODAS

        #Podriamos sacar filas que tengan vacias alguna de las columnas, sobre todo si son importantes para la query
 
        row_str = f"{movie_id}--{budget}--{overview}--{production_countries}--{revenue}--{title}"
        movies.append((movie_id, row_str))





queue_manager = QueueManagerPublisher()  
queue_manager.declare_exchange('gateway_metadata', 'direct')

for movie in movies:
    movie_id, row = movie
    queue_manager.publish_message(
        exchange_name='gateway_metadata', routing_key=str(movie_id[-1]), message=row)
    print(f" [x] Sending {movie_id} with key {movie_id[-1]}")


print(" [x] Sending EOF message")
queue_manager.publish_message(
    exchange_name='gateway_metadata', routing_key="-1", message=END)

queue_manager.close_connection()