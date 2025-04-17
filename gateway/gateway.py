#!/usr/bin/env python
import pika
import csv

movies = []

with open('movies_metadata.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) 
    for row in reader:
        movie_id = row[5] 
        row_str = ",".join(row)
        movies.append((movie_id, row_str))

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.exchange_declare(exchange='gateway_metadata', exchange_type='direct')

for movie in movies:
    movie_id, row = movie
    channel.basic_publish(
    exchange='gateway_metadata', routing_key=movie_id[-1], body=row)
    print(f" [x] Sending {movie_id}")

connection.close()