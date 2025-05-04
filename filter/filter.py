import constants
import node
import os

class Filter:
    def __init__(self):
        self.clients_ended = {}
        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback),
            ],
            node_id = os.getenv("NODE_ID", ""),
        )
        self.node_instance.start_consuming()

    def callback(self, _ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")
            self.end_when_bind_ends(method.routing_key, client)

            if client not in self.clients_ended:
                self.clients_ended[client] = 0
            self.clients_ended[client] += 1

            if self.clients_ended[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                self.end_when_all_binds_end(client)


        else:
            body_split = body.decode().split(constants.SEPARATOR)
            routing_key, row_str = self.filter(body_split)
            if routing_key and row_str:
                self.node_instance.send_message(
                    routing_key=routing_key,
                    message=row_str
                )

    def end_when_bind_ends(self, bind, client):
        self.node_instance.send_end_message(bind, client)

    def end_when_all_binds_end(self, client):
        pass

    def filter(self, body_split):
        raise NotImplementedError("Subclass responsibility")
    
    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Filter shutdown.")

if __name__ == '__main__':
    Filter()