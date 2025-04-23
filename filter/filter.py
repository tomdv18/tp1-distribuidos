import constants
import node
import os
from filter_function import filter_function

class Filter:
    def __init__(self):
        self.ended = 0

        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback),
            ]
        )
        self.node_instance.start_consuming()

    def callback(self, _ch, method, _properties, body):
        if body.decode() == constants.END:
            print(f" [*] Received EOF for bind {method.routing_key}")
            self.node_instance.send_end_message(method.routing_key)
            self.ended += 1
            if self.ended == self.node_instance.total_binds():
                print(" [*] Received EOF for all movies, exiting...")
                self.node_instance.stop_consuming_and_close_connection(0)
                self.node_instance.close_publisher_connection()
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            routing_key, row_str = filter_function(body_split)
            if routing_key and row_str:
                self.node_instance.send_message(
                    routing_key=routing_key,
                    message=row_str
                )

if __name__ == '__main__':
    Filter()