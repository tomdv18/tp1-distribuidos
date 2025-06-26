import constants
from generic import Generic
import json
import os

class AggregatorQ3(Generic):
    def __init__(self):
        self.top_rating = {}
        self.worst_rating = {}
        self.batch = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")
            
            if client not in self.clients_ended:
                self.clients_ended[client] = []

            if method.routing_key in self.clients_ended[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended[client].append(method.routing_key)

            if len(self.clients_ended[client]) == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                self.check_batch(client, last_eof=True)
                if client in self.top_rating and client in self.worst_rating:
                    message_id = self.generate_message_id()
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 3 -> {self.top_rating[client][0]} {self.top_rating[client][1]} {self.top_rating[client][2]}{constants.SEPARATOR}{client}"
                    )
                    message_id = self.generate_message_id()
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 3 -> {self.worst_rating[client][0]} {self.worst_rating[client][1]} {self.worst_rating[client][2]}{constants.SEPARATOR}{client}"
                    )
                self.node_instance.send_end_message('results', client)
                self.top_rating.pop(client, None)
                self.worst_rating.pop(client, None)
                self.clients_ended.pop(client, None)

            self.persist_eof()
            #self.persist_state()
            ch.basic_ack(delivery_tag=method.delivery_tag)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            rating = float(body_split[2])
            client = body_split[3]
            message_id = body_split[4] 
            node_id = body_split[5]
            if self.node_instance.is_repeated(message_id, client, node_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
        
            if client not in self.top_rating or rating > self.top_rating[client][2]:
                self.top_rating[client] = (movie_id, title, rating)
            if client not in self.worst_rating or rating < self.worst_rating[client][2]:
               self.worst_rating[client] = (movie_id, title, rating)

            if node_id not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[node_id] = {}
            self.node_instance.last_message_id[node_id][client] = body_split[-2]

            if client not in self.batch:
                self.batch[client] = []
            
            self.batch[client].append((ch, method))

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Aggregator Q3 shutdown.")
    
    def persist_state(self):
        try:
            with open(f'{constants.PATH}state.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps({
            "top_rating": self.top_rating,
            "worst_rating": self.worst_rating,
            "last_message_id": self.node_instance.last_message_id
        }) + "\n"
        lines.append(nueva_linea)
        
        
        lines = lines[-5:]
        
        temp_file = f'{constants.PATH}state.json.tmp'
        with open(temp_file, 'w') as archivo:
            archivo.writelines(lines)
        
        os.rename(temp_file, f'{constants.PATH}state.json')


        
    def load_custom_state(self, data):
        if "top_rating" in data:
            self.top_rating = data["top_rating"]
        if "worst_rating" in data:
            self.worst_rating = data["worst_rating"]
        if "last_message_id" in data:
            self.node_instance.last_message_id = data["last_message_id"]

if __name__ == '__main__':
    AggregatorQ3()