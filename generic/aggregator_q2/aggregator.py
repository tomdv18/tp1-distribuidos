import constants
import node
import os
from generic import Generic

class AggregatorQ2(Generic):
    def __init__(self):
        self.budgets = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")

            if client not in self.clients_ended:
                self.clients_ended[client] = 0
            self.clients_ended[client] += 1

            if self.clients_ended[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                top_five = sorted(
                    self.budgets[client].items(),
                    key=lambda x: (-x[1], x[0])
                )[:5]
                for country, budget in top_five:
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 2 -> {country} {budget}{constants.SEPARATOR}{client}"
                    )
                self.node_instance.send_end_message('results', client)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            country_name = body_split[0]
            budget = int(body_split[1]) 
            client = body_split[2]
            if client not in self.budgets:
                self.budgets[client] = {}
            if country_name not in self.budgets[client]:
                self.budgets[client][country_name] = budget
            else:
                self.budgets[client][country_name] += budget

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Aggregator Q2 shutdown.")
            

if __name__ == '__main__':
    AggregatorQ2()