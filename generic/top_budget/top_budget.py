import constants
import node
import os
from generic import Generic
import json
import time

class TopBudget(Generic):
    def __init__(self):
        self.budgets = {}
        self.batch = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):

        if body.decode().startswith(constants.CLIENT_TIMEOUT):
            client = body.decode()[len(constants.CLIENT_TIMEOUT):].strip()
            print(f" [*] Received timeout for client {client}")

            if client not in self.clients_timeout:
                self.clients_timeout[client] = time.time()
                self.persist_timeout()

            if client in self.clients_ended:
                print(f" [*] Removing client {client} from EOF list due to timeout.")
                self.clients_ended.pop(client, None)
                self.persist_eof()
            
            if client in self.budgets:
                print(f" [*] Removing client {client} from budgets due to timeout.")
                self.budgets.pop(client, None)
                self.persist_state()

            if client in self.batch:
                print(f" [*] Removing client {client} from batch due to timeout.")
                self.batch.pop(client, None)

            self.node_instance.send_timeout_message(
                routing_key=method.routing_key,
                client=client
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            if not self.should_process(client):
                print(f" [*] Ignoring EOF for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")

            
            if client not in self.clients_ended:
                self.clients_ended[client] = []

            if method.routing_key in self.clients_ended[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended[client].append(method.routing_key)

            if len(self.clients_ended[client]) == self.node_instance.total_binds():
                self.check_batch(client, last_eof=True)

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


            self.persist_eof()
            #self.persist_state()
            ch.basic_ack(delivery_tag=method.delivery_tag)


        else:
            body_split = body.decode().split(constants.SEPARATOR)
            country_name = body_split[0]
            budget = int(body_split[1]) 
            client = body_split[2]
            if not self.should_process(client):
                print(f" [*] Ignoring message for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            message_id = body_split[3]
            node_id = body_split[4]
            if self.node_instance.is_repeated(message_id, client, node_id):
                print(f" [*] Repeated message {message_id} from client {client} w/ node_id {node_id}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
            if client not in self.budgets:
                self.budgets[client] = {}
            if country_name not in self.budgets[client]:
                self.budgets[client][country_name] = budget
            else:
                self.budgets[client][country_name] += budget

            if client not in self.batch:
                self.batch[client] = []
            
            self.batch[client].append((ch, method))

            if node_id not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[node_id] = {}
            self.node_instance.last_message_id[node_id][client] = body_split[-2]

            self.check_batch(client)

    def persist_state(self):
        try:
            with open(f'{constants.PATH}state.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps({
            "budgets": self.budgets,
            "last_message_id": self.node_instance.last_message_id
        }) + "\n"
        lines.append(nueva_linea)
        
        # Mantener solo las últimas 5 líneas
        lines = lines[-5:]
        
        # Escribir a archivo temporal primero
        temp_file = f'{constants.PATH}state.json.tmp'
        with open(temp_file, 'w') as archivo:
            archivo.writelines(lines)
        
        # Mover atomicamente usando os.rename
        os.rename(temp_file, f'{constants.PATH}state.json')
    
    def load_custom_state(self, data):
        if "budgets" in data:
            self.budgets = data["budgets"]
        if "last_message_id" in data:
            self.node_instance.last_message_id = data["last_message_id"]
 

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Top shutdown.")
            

if __name__ == '__main__':
    TopBudget()