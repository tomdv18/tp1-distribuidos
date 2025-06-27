import time
import constants
import node
import os
import json

class Filter:
    def __init__(self):
        os.makedirs(constants.PATH, exist_ok=True)
        self.clients_ended = {}
        self.clients_timeout = {}
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
        eof_path = os.path.join(constants.PATH, "eof.json")
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
            

            self.persist_eof()

        else:
            body_split = body.decode().split(constants.SEPARATOR)

            routing_key, row_str = self.filter(body_split)
            if routing_key and row_str:
                self.node_instance.send_message(
                    routing_key=routing_key,
                    message=row_str
                )
        
    
        ch.basic_ack(delivery_tag=method.delivery_tag)
        

    def end_when_bind_ends(self, bind, client):
        self.node_instance.send_end_message(bind, client)

    def end_when_all_binds_end(self, client):
        pass

    def filter(self, body_split):
        raise NotImplementedError("Subclass responsibility")
    
    def should_process(self, client):
        return client not in self.clients_timeout
    
    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Filter shutdown.")

if __name__ == '__main__':
    Filter()