import constants
import node
import os
import json

class Generic:
    def __init__(self):#, custom_state=None):
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
        self.node_instance.start_consuming()

    def callback(self, ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")
    
    def generate_message_id(self):
        id = self.messages_sended
        self.messages_sended += 1
        return id
    
    def load_state(self):
        eof_path = os.path.join(constants.PATH, "eof.json")
        state_path = os.path.join(constants.PATH, "state.json")

        if os.path.isfile(eof_path):
            try:
                with open(eof_path, 'r') as f:
                    lines = f.readlines()
                
                # Intentar cargar desde la última línea hacia atrás
                for i in range(len(lines) - 1, -1, -1):
                    line = lines[i].strip()
                    if line:  # Si la línea no está vacía
                        try:
                            self.clients_ended = json.loads(line)
                            print(f"Cargado clients_ended desde línea {i+1} de {eof_path}")
                            break
                        except json.JSONDecodeError as e:
                            print(f"Error parseando línea {i+1} del EOF: {e}, intentando con anterior...")
                            continue
                else:
                    print("No se pudo cargar ninguna línea válida del EOF")
                    self.clients_ended = {}
                    
            except Exception as e:
                print(f"Error leyendo archivo EOF: {e}")
                self.clients_ended = {}
        else:
            print("No se encontró eof.json, iniciando vacío.")

        if os.path.isfile(state_path):
            try:
                with open(state_path, 'r') as f:
                    lines = f.readlines()
                
                # Intentar cargar desde la última línea hacia atrás
                for i in range(len(lines) - 1, -1, -1):
                    line = lines[i].strip()
                    if line:
                        try:
                            data = json.loads(line)
                            self.load_custom_state(data)
                            print(f"Cargado state desde línea {i+1} de {state_path}")
                            break
                        except Exception as e:
                            print(f"Error parseando línea {i+1}: {e}, intentando con anterior...")
                            continue
                else:
                    print("No se pudo cargar ninguna línea válida del state")
                    
            except Exception as e:
                print(f"Error leyendo archivo state: {e}")
        else:
            print("No se encontró state.json, iniciando vacío.")
    
    def load_custom_state(self, data):
        raise NotImplementedError("Subclass responsibility")
    
    def persist_state(self):
        raise NotImplementedError("Subclass responsibility")
    
    def check_batch(self, client, last_eof=False):
        if client not in self.batch:
            return
        client_batch = self.batch[client]

        if len(client_batch) >= constants.BATCH_SIZE or last_eof:
            self.persist_state()
            print("persisti estado")
            for ch, method in client_batch:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            print("fin de mandar acks")

            self.batch.pop(client, None)
    
    def persist_eof(self):
        try:
            with open(f'{constants.PATH}eof.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps(self.clients_ended) + "\n"
        lines.append(nueva_linea)
        
        # Mantener solo las últimas 5 líneas
        lines = lines[-5:]
        
        # Escribir a archivo temporal primero
        temp_file = f'{constants.PATH}eof.json.tmp'
        with open(temp_file, 'w') as archivo:
            archivo.writelines(lines)
        
        # Mover atomicamente usando os.rename
        os.rename(temp_file, f'{constants.PATH}eof.json')

if __name__ == '__main__':
    Generic()