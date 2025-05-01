import constants
import node
import os

class Join:
    def __init__(self):
        self.ended_metadata = 0
        self.ended_joined = 0
        self.results = {}

        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE_METADATA", ""), self.callback_metadata),
                (os.getenv("CONSUMER_EXCHANGE_JOINED", ""), self.callback_joined),
            ],
            node_id = os.getenv("NODE_ID", "")
        )
        self.node_instance.start_consuming()

    def callback_metadata(self, _ch, method, _properties, body):
        if body.decode() == constants.END:  
            print(f" [*] Received EOF for metadata bind {method.routing_key}")
            self.ended_metadata += 1
            if self.ended_metadata == self.node_instance.total_binds():
                print(" [*] Received EOF for all movies")
                self.node_instance.stop_consuming_and_close_connection(0)
                return
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            if movie_id not in self.results:
                self.results[movie_id] = (title, 0, 0)

    def callback_joined(self, _ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")
        
if __name__ == '__main__':
    Join()