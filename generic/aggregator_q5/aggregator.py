import constants
import node
import os
from generic import Generic

class AggregatorQ5(Generic):
    def __init__(self):
        self.results = {}
        self.cant = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):

        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            if client not in self.clients_ended:
                self.clients_ended[client] = 0
            self.clients_ended[client] += 1

            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")


            if self.clients_ended[client] == self.node_instance.total_binds():
                print(f"Client: {client} finished all binds.")

                
                for sentiment_label in self.results.get(client, {}):
                    count = self.cant[client].get(sentiment_label, 0)
                    if count != 0:
                        average = self.results[client][sentiment_label] / count
                        self.node_instance.send_message(
                            routing_key='results',
                            message=f"Query 5 -> {sentiment_label} {average}{constants.SEPARATOR}{client}"
                        )
                self.node_instance.send_end_message('results', client)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            sentiment_label = body_split[0]
            average = float(body_split[1])
            client = body_split[2]

            if client not in self.results:
                self.results[client] = {}
            if client not in self.cant:
                self.cant[client] = {}


                if sentiment_label not in self.results[client]:
                    self.results[client][sentiment_label] = 0
                self.results[client][sentiment_label] += average

                if sentiment_label not in self.cant[client]:
                    self.cant[client][sentiment_label] = 0
                self.cant[client][sentiment_label] += 1

                
if __name__ == '__main__':
    AggregatorQ5()