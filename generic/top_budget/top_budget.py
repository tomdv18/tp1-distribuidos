import constants
import node
import os
from generic import Generic
import json

class TopBudget(Generic):
    def __init__(self):
        self.budgets = {}
        super().__init__(self.leer_budgets)

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
                        message_id = self.generate_message_id()
                        self.node_instance.send_message(
                            routing_key="1",
                            message=f"{country}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
                        )
                        print(f" [*] Sending top 5 client {client}: {country} - {budget}")
                self.node_instance.send_end_message_to_all_binds(client)
                self.budgets.pop(client, None)
                self.clients_ended.pop(client, None)

            
            if client not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[client] = f'{constants.END}-{method.routing_key}'
            self.node_instance.last_message_id[client] = f'{constants.END}-{method.routing_key}'


            with open(f'{constants.PATH}clients_ended.json', 'w') as archivo:
                json.dump( self.clients_ended, archivo)

            with open(f'{constants.PATH}message_id.json', 'w') as archivo:
                json.dump(self.node_instance.last_message_id, archivo)
            
            with open(f'{constants.PATH}budgets.json', 'w') as archivo:
                json.dump(self.budgets, archivo)
            with open(f'{constants.PATH}messages_sended.txt', 'w') as archivo:
                archivo.write(str(self.messages_sended))

            

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            country_name = body_split[0]
            budget = int(body_split[1]) 
            client = body_split[2]
            message_id = body_split[3]
            node_id = body_split[4]
            if self.node_instance.is_repeated(message_id, client, node_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
            if client not in self.budgets:
                self.budgets[client] = {}
            if country_name not in self.budgets[client]:
                self.budgets[client][country_name] = budget
            else:
                self.budgets[client][country_name] += budget

            if node_id not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[node_id] = {}
            self.node_instance.last_message_id[node_id][client] = body_split[-2]

            with open(f'{constants.PATH}message_id.json', 'w') as archivo:
                json.dump(self.node_instance.last_message_id, archivo)
            
            with open(f'{constants.PATH}budgets.json', 'w') as archivo:
                json.dump(self.budgets, archivo)

            with open(f'{constants.PATH}messages_sended.txt', 'w') as archivo:
                archivo.write(str(self.messages_sended))

            
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Top shutdown.")

    def leer_budgets(self):
        budgets_path = os.path.join(constants.PATH, "budgets.json")
        if os.path.isfile(budgets_path):
            try:
                with open(budgets_path, 'r') as f:
                    self.budgets = json.load(f)
                print(f"Cargado budgets desde {budgets_path}")
            except Exception as e:
                print(f"Error cargando budgets: {e}")
                self.budgets = {}
        else:
            print("No se encontró budgets.json, iniciando vacío.")
            

if __name__ == '__main__':
    TopBudget()