import constants
import node
import os
from generic import Generic
import json

class AverageBudget(Generic):
    def __init__(self):
        self.results = {}
        self.cant = {}
        self.batch = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            if client not in self.clients_ended:
                self.clients_ended[client] = []
            if method.routing_key in self.clients_ended[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended[client].append(method.routing_key)

            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")


            if len(self.clients_ended[client]) == self.node_instance.total_binds():
                print(f"Client: {client} finished all binds.")
                # Tengo todos los EOFs
                self.check_batch(client, last_eof=True)
                
                for sentiment_label in self.results.get(client, {}):
                    count = self.cant[client].get(sentiment_label, 0)
                    if count != 0:
                        message_id = self.generate_message_id()
                        average = self.results[client][sentiment_label] / count
                        self.node_instance.send_message(
                            routing_key=method.routing_key,
                            message=f"{sentiment_label}{constants.SEPARATOR}{average}{constants.SEPARATOR}{count}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
                        )
                self.node_instance.send_end_message_to_all_binds(client)
                self.results.pop(client, None)
                self.cant.pop(client, None)
                self.clients_ended.pop(client, None)

            self.persist_eof()
            self.persist_state()
            ch.basic_ack(delivery_tag=method.delivery_tag)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            budget = body_split[1]
            revenue = body_split[2]
            sentiment_label = body_split[3]
            client = body_split[6]
            message_id = body_split[7]
            node_id = body_split[8]
            if self.node_instance.is_repeated(message_id, client, node_id):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                return

            if client not in self.results:
                self.results[client] = {}
            if client not in self.cant:
                self.cant[client] = {}

            budget = float(budget)
            revenue = float(revenue)

            if budget != 0 and revenue != 0:
                revenue_over_budget = revenue / budget

                if sentiment_label not in self.results[client]:
                    self.results[client][sentiment_label] = 0
                self.results[client][sentiment_label] += revenue_over_budget

                if sentiment_label not in self.cant[client]:
                    self.cant[client][sentiment_label] = 0
                self.cant[client][sentiment_label] += 1
    
        
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
            "results": self.results,
            "cant": self.cant,
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
        if "results" in data:
            self.results = data["results"]
        if "cant" in data:
            self.cant = data["cant"]
        if "last_message_id" in data:
            self.node_instance.last_message_id = data["last_message_id"]

if __name__ == '__main__':
    AverageBudget()