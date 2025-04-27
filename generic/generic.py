import constants
import node
import os

class Generic:
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

    def callback(self, ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")

if __name__ == '__main__':
    Generic()