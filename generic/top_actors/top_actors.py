import constants
import node
import os
from generic import Generic
import json
import time

class TopActors(Generic):
    def __init__(self):
        self.ocurrences = {}
        self.batch = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.CLIENT_TIMEOUT):
            client = body.decode()[len(constants.CLIENT_TIMEOUT):].strip()
            self.check_batch(client, last_eof=True)
            print(f" [*] Received timeout for client {client}")

            if client not in self.clients_timeout:
                self.clients_timeout[client] = time.time()
                self.persist_timeout()

            if client in self.clients_ended:
                print(f" [*] Removing client {client} from EOF list due to timeout.")
                self.clients_ended.pop(client, None)
                self.persist_eof()

            state_changed = False

            if client in self.ocurrences:
                print(f" [*] Removing client {client} from occurrences due to timeout.")
                self.ocurrences.pop(client, None)
                state_changed = True

            for node_id in self.node_instance.last_message_id:
                if self.node_instance.last_message_id[node_id].pop(client, None) is not None:
                    state_changed = True

            if state_changed:
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
            body_split = body.decode().split(constants.SEPARATOR)
            client = body_split[1]
            node_id = body_split[2]
            if not self.should_process(client):
                print(f" [*] Ignoring EOF for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            print(f" [*] Received EOF for bind {method.routing_key} from client {client} from {node_id}")
            
            if client not in self.clients_ended:
                self.clients_ended[client] = []

            combination = method.routing_key + node_id
            if combination in self.clients_ended[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} from node_id {node_id} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended[client].append(combination)

            if client not in self.ocurrences:
                self.ocurrences[client] = {} 
            
            if len(self.clients_ended[client]) == self.node_instance.total_binds() * int(os.getenv('EOF', '0')):
                self.check_batch(client, last_eof=True)
                print(f" [*] Client {client} finished all binds.")
                top_ten = sorted(
                    self.ocurrences[client].items(),
                    key=lambda x: (-x[1][0], x[1][1])
                )[:10]
                for id, (count, name) in top_ten:
                    message_id = self.generate_message_id()
                    self.node_instance.send_message(
                        routing_key=str(id[-1]),
                        message=f"{id}{constants.SEPARATOR}{count}{constants.SEPARATOR}{name}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
                    )

                self.node_instance.send_end_message_to_all_binds(client)
                self.ocurrences.pop(client, None)
                self.clients_ended.pop(client, None)
                for node_id in self.node_instance.last_message_id:
                    self.node_instance.last_message_id[node_id].pop(client, None)

            self.persist_eof()
            self.persist_state()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            id = body_split[0]
            name = body_split[1]
            client = body_split[3]
            if not self.should_process(client):
                print(f" [*] Ignoring message for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            message_id = body_split[4]
            node_id = body_split[5]
            if self.node_instance.is_repeated(message_id, client, node_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
            if client not in self.ocurrences:
                self.ocurrences[client] = {}
            if id not in self.ocurrences[client]:
                self.ocurrences[client][id] = [1, name]
            else:
                self.ocurrences[client][id][0] += 1

            if node_id not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[node_id] = {}
            self.node_instance.last_message_id[node_id][client] = body_split[-2]

            if client not in self.batch:
                self.batch[client] = []
            
            self.batch[client].append((ch, method))

            self.check_batch(client)

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Top shutdown.")
    
    def persist_state(self):
        try:
            with open(f'{constants.PATH}state.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps({
            "ocurrences": self.ocurrences,
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
        if "ocurrences" in data:
            self.ocurrences = data["ocurrences"]
        if "last_message_id" in data:
            self.node_instance.last_message_id = data["last_message_id"]

if __name__ == '__main__':
    TopActors()