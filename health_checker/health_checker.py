import os
import socket
import time
from multiprocessing import Process, Manager
import subprocess

class HealthChecker():
    def __init__(self):
        port = int(os.environ.get("PORT", 8000))
        self.id = os.environ.get("ID", "1")
        self.hc_size = int(os.environ.get("HC_SIZE", 1))
        self.container_id = os.environ.get("CONTAINER_NAME", os.environ.get("HOSTNAME", "unknown"))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        print(f"Escuchando UDP en el puerto {port} con ID {self.id} y HC_SIZE {self.hc_size}. Contenedor: {self.container_id}")

    def listen(self, alive_containers, hc_containers, hc_containers_reset):
        while True:
            data, _addr = self.sock.recvfrom(1024)
            data = data.decode().strip()
            if data.startswith("HC"):
                self.handle_hc_data(data, alive_containers, hc_containers, hc_containers_reset)
            else:
                node_id = data
                alive_containers[node_id] = time.time()
                print(f"Recibido heartbeat de {node_id}")

    def handle_hc_data(self, data, alive_containers, hc_containers, hc_containers_reset):
        data_parts = data.split("-") # Formato: HC_ID-CONTAINER_ID-PROCESOS_DEL_HC_QUE_RECIBE-PROCESOS_DEL_HC_QUE_MANDA // HC_ID-RESET-HC_ID_DEL_QUE_REINICIAR
        hc_id = data_parts[0][2:] # Ej: HC1
        if hc_id in hc_containers_reset:
            print(f"El contenedor {hc_containers[hc_id][0]} de HC {hc_id} ya fue reiniciado por timeout por {hc_containers_reset[hc_id][0]} y ya se recibio un nuevo mensaje.")
            del hc_containers_reset[hc_id] # Elimino el contenedor de HC que ya fue reiniciado por timeout
        if data_parts[1] == "RESET":
            print(f"Recibido mensaje de reinicio de HC {hc_id} para el HC {data_parts[2]}")
            self.reset_msg_arrived(hc_id, hc_containers_reset, data_parts[2])
        else:
            container_id = data_parts[1]
            processes_this_hc = data_parts[2] if len(data_parts) > 2 else ""
            processes_other_hc = data_parts[3] if len(data_parts) > 3 else ""

            print(f"Recibido mensaje de salud de HC {hc_id}: Contenedor {container_id}, Procesos del HC que recibe: {processes_this_hc}, Procesos del HC que manda: {processes_other_hc}")

            for process in filter(None, processes_this_hc.split(",")):
                if process not in alive_containers:
                    alive_containers[process] = time.time()

            container_id_prev, processes, _last_seen = hc_containers.get(hc_id, ("", set(), time.time()))
            # Unir solo procesos no vacíos
            new_processes = processes.union(filter(None, processes_other_hc.split(",")))
            hc_containers[hc_id] = (container_id, new_processes, time.time())

    def reset_msg_arrived(self, hc_id, hc_containers_reset, hc_reset_id):
        if hc_reset_id not in hc_containers_reset:
            hc_containers_reset[hc_reset_id] = (hc_id, time.time())  # Si no estaba, lo agrego
        else:
            if int(hc_containers_reset[hc_reset_id][0]) > int(hc_id): # Si el id del HC que estaba es mas grande, actualizo por el mas chico
                hc_containers_reset[hc_reset_id] = (hc_id, time.time())

    def check_health(self, alive_containers, hc_containers_reset, hc_containers, timeout=10):
        while True:
            now = time.time()
            for node_id, last_seen in list(alive_containers.items()):
                if now - last_seen > timeout:
                    print(f"Nodo {node_id} está caído. Intentando reiniciarlo...")
                    try:
                        ps_before = subprocess.run(["docker", "ps"], capture_output=True, text=True)
                        print("[docker ps antes]:\n" + ps_before.stdout)
                        result = subprocess.run(["docker", "restart", node_id], capture_output=True, text=True)
                        if result.returncode == 0:
                            print(f"Contenedor {node_id} reiniciado correctamente.")
                        else:
                            print(f"Error al reiniciar {node_id}: {result.stderr}")
                        ps_after = subprocess.run(["docker", "ps"], capture_output=True, text=True)
                        print("[docker ps después]:\n" + ps_after.stdout)
                    except Exception as e:
                        print(f"Excepción al intentar reiniciar {node_id}: {e}")
                    del alive_containers[node_id]
            for hc_id, (container_id, processes, last_seen) in list(hc_containers.items()): #Itero por los contenedores de HC
                if now - last_seen > timeout:
                    if not container_id:
                        print(f"No se puede reiniciar el HC {hc_id} porque no se conoce su container_id. Esperando mensaje de salud.")
                        continue
                    self.check_reset_hc(hc_id, container_id, hc_containers_reset, hc_containers, timeout)
                    continue
                print("Contenedores activos en el HC:", processes)
                print("Contenedores activos:", list(alive_containers.keys()))
                msg = f"HC{self.id}-{self.container_id}-" + ",".join(processes) + "-" + ",".join(alive_containers.keys())
                health_checker = f"health_checker_{hc_id}"
                print(f"Enviando mensaje de salud: {msg} al HC {health_checker}")
                try:
                    self.sock.sendto(msg.encode(), (health_checker, 8000))
                except Exception as e:
                    print(f"Error al enviar mensaje de salud al HC {hc_id}: {e}")
            time.sleep(5)
    
    def check_reset_hc(self, hc_id_to_reset, container_id, hc_containers_reset, hc_containers, timeout):
        if hc_id_to_reset not in hc_containers_reset: #Si no esta a priori se encargara self.id de reiniciar
            hc_containers_reset[hc_id_to_reset] = (self.id, time.time())

        else: # Si ya estaba
            if time.time() - hc_containers_reset[hc_id_to_reset][1] > 2 * timeout: #Caso borde: se muere HC 3, HC 1 toma el reinicio pero antes de reiniciarlo muere, se borra de los containers a reset asi lo toma el HC 2 y lo terminara reniciando
                print(f"Vuelve a comenzar conteo para HC {hc_id_to_reset} borrandolo.")
                del hc_containers_reset[hc_id_to_reset]
                return

            if hc_containers_reset[hc_id_to_reset][0] == self.id: # si el id es el mismo
                if time.time() - hc_containers_reset[hc_id_to_reset][1] > timeout: # si cumplio el timeout reinicia
                    print(f"Reiniciando contenedor {container_id} del HC {hc_id_to_reset} por timeout.")
                    try:
                        result = subprocess.run(["docker", "restart", container_id], capture_output=True, text=True)
                        if result.returncode == 0:
                            print(f"Contenedor HC {container_id} reiniciado correctamente.")
                        else:
                            print(f"Error al reiniciar HC {container_id}: {result.stderr}")
                    except Exception as e:
                        print(f"Excepción al intentar reiniciar HC {container_id}: {e}")
                    del hc_containers_reset[hc_id_to_reset]
                    return #No quiero enviar nada si ya reinicie yo
            elif int(hc_containers_reset[hc_id_to_reset][0]) < int(self.id): # Container mas chico reinicia
                return #No quiero enviar si nada si no voy a reiniciar yo
            else: #self.id es mas chico, encargado del reinicio
                hc_containers_reset[hc_id_to_reset] = (self.id, time.time())
        
        for hc_id, (container_id, _processes, _last_seen) in hc_containers.items(): #Manda mensaje a todos para avisar que lleva el reinicio
            if hc_id != self.id and hc_id != hc_id_to_reset:
                msg = f"HC{self.id}-RESET-{hc_id_to_reset}"
                health_checker = f"health_checker_{hc_id}"
                print(f"Enviando mensaje de reinicio: {msg} al HC {health_checker}")
                try:
                    self.sock.sendto(msg.encode(), (health_checker, 8000))
                except Exception as e:
                    print(f"Error al enviar mensaje de reinicio al HC {hc_id}: {e}")

    def start(self):
        with Manager() as manager:
            alive_containers = manager.dict()
            hc_containers = manager.dict()
            for i in range(1, self.hc_size + 1):
                if i != int(self.id):
                    hc_containers[str(i)] = ("", set(), time.time()) # Inicializo los contenedores de HC con un set vacio de procesos y el tiempo actual
            
            print(f"Contenedores de HC inicializados: {hc_containers}")

            hc_containers_reset = manager.dict() # Para almacenar contenedores que necesitan reinicio. Formato: {hc_id: (manager_hc_id, last_msg)}
            p1 = Process(target=self.listen, args=(alive_containers, hc_containers, hc_containers_reset))
            p2 = Process(target=self.check_health, args=(alive_containers, hc_containers_reset, hc_containers, 20))
            p1.start()
            p2.start()
            p1.join()
            p2.join()

if __name__ == "__main__":
    hc = HealthChecker()
    hc.start()

