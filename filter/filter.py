import constants
import node
import os

class Filter:
    def __init__(self):
        self.ended = 0

        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback),
            ],
            node_id = os.getenv("NODE_ID", ""),
            client_count = int(os.getenv("CLIENT_COUNT", 1))
        )
        self.clients_ended = {}
        self.node_instance.start_consuming()

    def callback(self, _ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()

            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")
            self.end_when_bind_ends(method.routing_key, client)
            if client not in self.clients_ended:
                self.clients_ended[client] = 0

            self.clients_ended[client] += 1
            self.ended += 1


            all_clients_done = all(
                ended == self.node_instance.total_binds()
                for ended in self.clients_ended.values()
            )

            if all_clients_done and self.node_instance.client_count == len(self.clients_ended):
                print(" [*] All clients finished all binds. Shutting down...")
                self.end_when_all_binds_end(self.clients_ended)
                self.node_instance.stop_consuming_and_close_connection(0)
                self.node_instance.close_publisher_connection()

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            routing_key, row_str = self.filter(body_split)
            if routing_key and row_str:
                self.node_instance.send_message(
                    routing_key=routing_key,
                    message=row_str
                )

    def end_when_bind_ends(self, bind, clientAddr):
        self.node_instance.send_end_message(bind, clientAddr)

    def end_when_all_binds_end(self, clientAddr):
        pass

    def filter(self, body_split):
        raise NotImplementedError("Subclass responsibility")

if __name__ == '__main__':
    Filter()