import constants
import node
import os

class Join:
    def __init__(self):
        self.clients_ended_metadata = {}
        self.clients_ended_joined = {}
        self.results = {}
        self.waiting = {}
        self.finished = []
        self.message_count = 0

        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE_METADATA", ""), self.callback_metadata),
                (os.getenv("CONSUMER_EXCHANGE_JOINED", ""), self.callback_joined),
            ],
            node_id = os.getenv("NODE_ID", ""),
        )
        self.node_instance.start_consuming()

    def callback_metadata(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for metadata bind {method.routing_key} from client {client}")
            if client not in self.clients_ended_metadata:
                self.clients_ended_metadata[client] = 0
            self.clients_ended_metadata[client] += 1
            if self.clients_ended_metadata[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all metadata binds.")
                if client in self.clients_ended_joined and self.clients_ended_joined[client] == self.node_instance.total_binds():
                    self.send_pending(client)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            client = body_split[2]
            message_id = body_split[3]
            if self.node_instance.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
            if client not in self.results:
                self.results[client] = {}
            if movie_id not in self.results[client]:
                self.results[client][movie_id] = (title, 0, 0)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_joined(self, _ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")
    
    def send_pending(self, client):
        raise NotImplementedError("Subclass responsibility")
    
    def remove_client(self, client):
        self.clients_ended_joined.pop(client, None)
        self.clients_ended_metadata.pop(client, None)
        self.results.pop(client, None)
        self.waiting.pop(client, None)
    
    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Join shutdown.")
    
    def generate_message_id(self, prefix):
        id = f"{prefix}-{self.message_count:07d}"
        self.message_count += 1 
        return id

        
if __name__ == '__main__':
    Join()