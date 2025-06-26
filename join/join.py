import constants
import node
import os
import json
import time

class Join:
    def __init__(self):
        self.clients_ended_metadata = {}
        self.clients_ended_joined = {}
        self.results = {}
        self.waiting = {}
        self.finished = []
        self.clients_timeout = {}
        self.batch = {}


        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE_METADATA", ""), self.callback_metadata),
                (os.getenv("CONSUMER_EXCHANGE_JOINED", ""), self.callback_joined),
            ],
            node_id = os.getenv("NODE_ID", ""),
        )
        self.load_state()
        self.node_instance.start_consuming()

    def callback_metadata(self, ch, method, _properties, body):

        if body.decode().startswith(constants.CLIENT_TIMEOUT):
            client = body.decode()[len(constants.CLIENT_TIMEOUT):].strip()
            print(f" [*] Received timeout for client {client}")

            if client not in self.clients_timeout:
                self.clients_timeout[client] = time.time()
                self.persist_timeout()
                
            state_changed = False
            eof_changed = False
            if client in self.clients_ended_metadata:
                print(f" [*] Removing client {client} from metadata binds due to timeout.")
                self.clients_ended_metadata.pop(client, None)
                eof_changed = True

            if client in self.clients_ended_joined:
                print(f" [*] Removing client {client} from joined binds due to timeout.")
                self.clients_ended_joined.pop(client, None)
                eof_changed = True
            
            if client in self.results:
                print(f" [*] Removing client {client} from results due to timeout.")
                self.results.pop(client, None)
                state_changed = True

            if client in self.waiting:
                print(f" [*] Removing client {client} from waiting due to timeout.")
                self.waiting.pop(client, None)
                state_changed = True

            if state_changed:
                self.persist_state()
            if eof_changed:
                self.persist_eof()

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
            print(f" [*] Received EOF for metadata bind {method.routing_key} from client {client}")
            if client not in self.clients_ended_metadata:
                self.clients_ended_metadata[client] = []

            if method.routing_key in self.clients_ended_metadata[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended_metadata[client].append(method.routing_key)

            if len(self.clients_ended_metadata[client]) == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all metadata binds.")
                self.check_batch(client, last_eof=True)
                if client in self.clients_ended_joined and len(self.clients_ended_joined[client]) == self.node_instance.total_binds():
                    self.send_pending(client)

            self.persist_eof()
            #self.persist_state()
            ch.basic_ack(delivery_tag=method.delivery_tag)

        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            client = body_split[2]
            message_id = body_split[3]
            if not self.should_process(client):
                print(f" [*] Ignoring metadata for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if client not in self.results:
                self.results[client] = {}
            if movie_id not in self.results[client]:
                self.results[client][movie_id] = (title, 0, 0, message_id)
        
            if client not in self.batch:
                self.batch[client] = []
            
            self.batch[client].append((ch, method))

            self.check_batch(client)

    def callback_joined(self, _ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")
    
    def send_pending(self, client):
        raise NotImplementedError("Subclass responsibility")
    
    def remove_client(self, client):
        self.clients_ended_joined.pop(client, None)
        self.clients_ended_metadata.pop(client, None)
        self.results.pop(client, None)
        self.waiting.pop(client, None)
    
    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Join shutdown.")

    def check_batch(self, client, last_eof=False):
        if client not in self.batch:
            return
        client_batch = self.batch[client]

        if len(client_batch) >= constants.BATCH_SIZE or last_eof:
            self.persist_state()
            #print("persisti estado")
            #import time
            #time.sleep(0.2)
            for ch, method in client_batch:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            #print("fin de mandar acks")

            self.batch.pop(client, None)
        
    def should_process(self, client):
        return client not in self.clients_timeout
    

    def load_state(self):
        eof_path = os.path.join(constants.PATH, "eof.json")
        state_path = os.path.join(constants.PATH, "state.json")
        timeout_path = os.path.join(constants.PATH, "timeout.json")


        if os.path.isfile(eof_path):
            try:
                with open(eof_path, 'r') as f:
                    lines = f.readlines()
                
                # Intentar cargar desde la última línea hacia atrás
                for i in range(len(lines) - 1, -1, -1):
                    line = lines[i].strip()
                    if line:  # Si la línea no está vacía
                        try:
                            data = json.loads(line)
                            if "clients_ended_metadata" in data:
                                self.clients_ended_metadata = data["clients_ended_metadata"]
                            if "clients_ended_joined" in data:
                                self.clients_ended_joined = data["clients_ended_joined"]
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

        if os.path.isfile(timeout_path):
            try:
                with open(timeout_path, 'r') as f:
                    lines = f.readlines()
                
                # Intentar cargar desde la última línea hacia atrás
                for i in range(len(lines) - 1, -1, -1):
                    line = lines[i].strip()
                    if line:  # Si la línea no está vacía
                        try:
                            self.clients_timeout = json.loads(line)
                            print(f"Cargado clients_timeout desde línea {i+1} de {timeout_path}")
                            break
                        except json.JSONDecodeError as e:
                            print(f"Error parseando línea {i+1} del timeout: {e}, intentando con anterior...")
                            continue
                else:
                    print("No se pudo cargar ninguna línea válida del timeout")
                    self.clients_timeout = {}
                    
            except Exception as e:
                print(f"Error leyendo archivo timeout: {e}")
                self.clients_timeout = {}
        else:
            print("No se encontró timeout.json, iniciando vacío.")



    def load_custom_state(self, data):
        if "results" in data:
            self.results = data["results"]
        if "waiting" in data:
            self.waiting = data["waiting"]
        if "finished" in data:
            self.finished = data["finished"]
        if "last_message_id" in data:
            self.node_instance.last_message_id = data["last_message_id"]
    
    def persist_state(self):
        try:
            with open(f'{constants.PATH}state.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps({
            "top_rating": self.top_rating,
            "worst_rating": self.worst_rating,
            "finished": self.finished,
            "last_message_id": self.node_instance.last_message_id
        }) + "\n"
        lines.append(nueva_linea)
        
        lines = lines[-5:]
        
        temp_file = f'{constants.PATH}state.json.tmp'
        with open(temp_file, 'w') as archivo:
            archivo.writelines(lines)
        
        os.rename(temp_file, f'{constants.PATH}state.json')
    
    def persist_eof(self):
        try:
            with open(f'{constants.PATH}eof.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps({
            "clients_ended_metadata": self.clients_ended_metadata,
            "clients_ended_joined": self.clients_ended_joined
        }) + "\n"
        lines.append(nueva_linea)
        
        # Mantener solo las últimas 5 líneas
        lines = lines[-5:]
        
        # Escribir a archivo temporal primero
        temp_file = f'{constants.PATH}eof.json.tmp'
        with open(temp_file, 'w') as archivo:
            archivo.writelines(lines)
        
        # Mover atomicamente usando os.rename
        os.rename(temp_file, f'{constants.PATH}eof.json')

    def persist_timeout(self):
        try:
            with open(f'{constants.PATH}timeout.json', 'r') as archivo:
                lines = archivo.readlines()
        except FileNotFoundError:
            lines = []
        
        nueva_linea = json.dumps(self.clients_timeout) + "\n"
        lines.append(nueva_linea)
        
        # Mantener solo las últimas 5 líneas
        lines = lines[-5:]
        
        # Escribir a archivo temporal primero
        temp_file = f'{constants.PATH}timeout.json.tmp'
        with open(temp_file, 'w') as archivo:
            archivo.writelines(lines)
        
        # Mover atomicamente usando os.rename
        os.rename(temp_file, f'{constants.PATH}timeout.json')


        
if __name__ == '__main__':
    Join()