from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants
import os

class Node:
    def __init__(self, publisher_exchange, binds, consumer_exchanges_and_callbacks):
        self.publisher_exchange = publisher_exchange
        self.binds = binds
        self.consumer_exchanges_and_callbacks = consumer_exchanges_and_callbacks
        self.ended = [0] * len(consumer_exchanges_and_callbacks)
        self.publisher = self.declare_publisher()
        self.consumers = self.declare_consumers()
    
    def declare_consumers(self):
        queue_manager_inputs = []
        for exchange, callback in self.consumer_exchanges_and_callbacks:
            queue_manager_input = QueueManagerConsumer()
            queue_manager_input.declare_exchange(exchange_name=exchange, exchange_type='direct')
            queue_name = queue_manager_input.queue_declare(queue_name='')
            for bind in self.binds:
                queue_manager_input.queue_bind(
                    exchange_name=exchange, queue_name=queue_name, routing_key=bind)
                print(f" [*] Asociated with bind: {bind}")
            queue_manager_input.consume_messages(
                queue_name,
                callback=callback
            )
            queue_manager_inputs.append(queue_manager_input)
        return queue_manager_inputs
    
    def declare_publisher(self):
        queue_manager_output = QueueManagerPublisher()  
        queue_manager_output.declare_exchange(self.publisher_exchange, 'direct')
        return queue_manager_output
    
    def start_consuming(self):
        for consumer in self.consumers:
            consumer.start_consuming()
    
    def send_end_message(self, routing_key):
        self.publisher.publish_message(
            exchange_name=self.publisher_exchange,
            routing_key=routing_key,
            message=constants.END
        )
        print(f" [*] Sending EOF for bind {routing_key}")

    def send_end_message_to_all_binds(self):
        for bind in self.binds:
            self.send_end_message(bind)

    def total_binds(self):
        return len(self.binds)

    def stop_consuming_and_close_connection(self, consumer):
        self.consumers[consumer].stop_consuming()
        self.consumers[consumer].close_connection()

    def close_publisher_connection(self):
        self.publisher.close_connection()

    def send_message(self, routing_key, message):
        self.publisher.publish_message(
            exchange_name=self.publisher_exchange,
            routing_key=routing_key,
            message=message
        )
         
        