import constants
import node
import os
from generic import Generic

class TopBudget(Generic):
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

            if self.clients_ended[client] == int(os.getenv("EOF", "0")):
                print(f" [*] Client {client} finished all binds.")
                if client in self.budgets:
                    top_five = sorted(
                        self.budgets[client].items(),
                        key=lambda x: (-x[1], x[0])
                    )[:5]
                    for country, budget in top_five:
                        message_id = self.generate_message_id(constants.TOP_BUDGET)
                        self.node_instance.send_message(
                            routing_key="1",
                            message=f"{country}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                        )
                        print(f" [*] Sending top 5 client {client}: {country} - {budget}")
                self.node_instance.send_end_message_to_all_binds(client)
                self.budgets.pop(client, None)
                self.clients_ended.pop(client, None)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            country_name = body_split[0]
            budget = int(body_split[1]) 
            client = body_split[2]
            message_id = body_split[3]
            if self.node_instance.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
            if client not in self.budgets:
                self.budgets[client] = {}
            if country_name not in self.budgets[client]:
                self.budgets[client][country_name] = budget
            else:
                self.budgets[client][country_name] += budget
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Top shutdown.")
            

if __name__ == '__main__':
    TopBudget()