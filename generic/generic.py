import constants
import node
import os
import json

class Generic:
    def __init__(self, custom_state=None):
        self.clients_ended = {}
        self.messages_sended = 0
        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback),
            ],
            node_id = os.getenv("NODE_ID", ""),
        )
        self.load_state()
        if custom_state:
            print("Cargando estado personalizado...")
            custom_state()
        self.node_instance.start_consuming()

    def callback(self, ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")
    
    def generate_message_id(self,prefix):
        #RETORNA UN STRING UNICO PARA CADA MENSAJE CON EL SIGUIENTE FORMATO PP-XXXXXXX
        id = f"{prefix}-{self.messages_sended:07d}"
        self.messages_sended += 1
        return id
    


    def load_state(self):
        clients_ended_path = os.path.join(constants.PATH, "clients_ended.json")
        message_id_path = os.path.join(constants.PATH, "message_id.json")
        messages_sended_path = os.path.join(constants.PATH, "messages_sended.txt")

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

        if os.path.isfile(messages_sended_path):
            try:
                with open(messages_sended_path, 'r') as f:
                    self.messages_sended = int(f.read().strip())
                print(f"Cargado messages_sended desde {messages_sended_path}")
            except Exception as e:
                print(f"Error cargando messages_sended: {e}")
                self.messages_sended = 0

if __name__ == '__main__':
    Generic()