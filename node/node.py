from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants
from still_alive import StillAlive
import os
from multiprocessing import Process, Event

class Node:
    def __init__(self, publisher_exchange, binds, consumer_exchanges_and_callbacks, node_id):
        self.publisher_exchange = publisher_exchange
        self.binds = binds
        self.consumer_exchanges_and_callbacks = consumer_exchanges_and_callbacks
        self.node_id = node_id
        self.ended = [0] * len(consumer_exchanges_and_callbacks)
        self.publisher = self.declare_publisher()
        self.consumer = self.declare_consumer()
        self.still_alive = StillAlive()
        self.still_alive.start()
        self.last_message_id = {}

    def declare_consumer(self):
        queue_manager_input = QueueManagerConsumer()
        for exchange, callback in self.consumer_exchanges_and_callbacks:
            queue_manager_input.declare_exchange(exchange_name=exchange, exchange_type='direct')
            queue_name = queue_manager_input.queue_declare(queue_name=f"{exchange}_input_queue_{self.node_id}")
            for bind in self.binds:
                queue_manager_input.queue_bind(
                    exchange_name=exchange, queue_name=queue_name, routing_key=bind)
                print(f" [*] Asociated with bind: {bind}")
            queue_manager_input.consume_messages(
                queue_name,
                callback=callback
            )
        return queue_manager_input
    
    def declare_publisher(self):
        queue_manager_output = QueueManagerPublisher()  
        queue_manager_output.declare_exchange(self.publisher_exchange, 'direct')
        return queue_manager_output
    
    def start_consuming(self):
        self.consumer.start_consuming()
    
    def send_end_message(self, routing_key, client):
        self.publisher.publish_message(
            exchange_name=self.publisher_exchange,
            routing_key=routing_key,
            message=f"{constants.END} {client}"
        )
        print(f" [*] Sending EOF for bind {routing_key}")

    def send_end_message_to_all_binds(self, client):
        for bind in self.binds:
            self.send_end_message(bind, client)

    def send_timeout_message(self, routing_key, client):
        self.publisher.publish_message(
            exchange_name=self.publisher_exchange,
            routing_key=routing_key,
            message=f"{constants.CLIENT_TIMEOUT} {client}"
        )
        print(f" [*] Sending CLIENT_TIMEOUT for bind {routing_key}")

    def send_timeout_message_to_all_binds(self, client):
        for bind in self.binds:
            self.send_timeout_message(bind, client)


    def total_binds(self):
        return len(self.binds)
    
    def stop_consuming_and_close_connection(self):
        self.consumers.stop_consuming()
        self.consumers.close_connection()
        self.still_alive.stop()

    def close_publisher_connection(self):
        self.publisher.close_connection()

    def send_message(self, routing_key, message):
        self.publisher.publish_message(
            exchange_name=self.publisher_exchange,
            routing_key=routing_key,
            message=message
        )

    def id(self):
        return self.node_id

    def is_repeated(self, message_id, client_id, node_id):
        if node_id in self.last_message_id and client_id in self.last_message_id[node_id]:
            last_message = self.last_message_id[node_id][client_id]
            return int(message_id) <= int(last_message)
        return False
