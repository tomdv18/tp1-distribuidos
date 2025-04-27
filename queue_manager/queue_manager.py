import pika

class QueueManager:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=3600, blocked_connection_timeout=3600))
        self.channel = self.connection.channel()

    def declare_exchange(self, exchange_name, exchange_type='direct'):
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type)
    
    def queue_declare(self, queue_name, exclusive=True):
        result = self.channel.queue_declare(queue=queue_name, exclusive=exclusive)
        return result.method.queue

    def close_connection(self):
        self.connection.close()

class QueueManagerPublisher(QueueManager):
    def publish_message(self, exchange_name, routing_key, message):
        self.channel.basic_publish(
            exchange=exchange_name, routing_key=routing_key, body=message)

class QueueManagerConsumer(QueueManager):
    def queue_bind(self, exchange_name, queue_name, routing_key):
        self.channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=routing_key)

    def consume_messages(self, queue_name, callback):
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

    def start_consuming(self):
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()





