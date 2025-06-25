import time
import constants
import node
import os
import json

class Filter:
    def __init__(self):
        os.makedirs(constants.PATH, exist_ok=True)
        self.clients_ended = {}
        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback),
            ],
            node_id = os.getenv("NODE_ID", ""),
        )
        self.load_state()

        self.node_instance.start_consuming()

    def load_state(self):
        clients_ended_path = os.path.join(constants.PATH, "clients_ended.json")
        message_id_path = os.path.join(constants.PATH, "message_id.json")

        if os.path.isfile(clients_ended_path):
            try:
                with open(clients_ended_path, 'r') as f:
                    self.clients_ended = json.load(f)
                print(f"Cargado clients_ended desde {clients_ended_path}")
            except Exception as e:
                print(f"Error cargando clients_ended: {e}")
                self.clients_ended = {}
        else:
            print("No se encontró clients_ended.json, iniciando vacío.")

        if os.path.isfile(message_id_path):
            try:
                with open(message_id_path, 'r') as f:
                    self.node_instance.last_message_id = json.load(f)
                print(f"Cargado last_message_id desde {message_id_path}")
            except Exception as e:
                print(f"Error cargando last_message_id: {e}")
                self.node_instance.last_message_id = {}
        else:
            print("No se encontró message_id.json, iniciando vacío.")

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(body.decode())
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")

            
            if client not in self.clients_ended:
                self.clients_ended[client] = []

            if method.routing_key in self.clients_ended[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended[client].append(method.routing_key)


            self.end_when_bind_ends(method.routing_key, client)
            if len(self.clients_ended[client]) == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                self.end_when_all_binds_end(client)
                self.clients_ended.pop(client, None)
            
            
            if client not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[client] = f'{constants.END}-{method.routing_key}'
            self.node_instance.last_message_id[client] = f'{constants.END}-{method.routing_key}'


            with open(f'{constants.PATH}clients_ended.json', 'w') as archivo:
                json.dump( self.clients_ended, archivo)

            with open(f'{constants.PATH}message_id.json', 'w') as archivo:
                json.dump(self.node_instance.last_message_id, archivo)
            

             

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            routing_key, row_str = self.filter(body_split)
            if routing_key and row_str:
                self.node_instance.send_message(
                    routing_key=routing_key,
                    message=row_str
                )

            with open(f'{constants.PATH}message_id.json', 'w') as archivo:
                    json.dump(self.node_instance.last_message_id, archivo)
        
        
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        

            

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