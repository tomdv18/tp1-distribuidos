import os
import socket
import time
from multiprocessing import Process, Manager
import subprocess

class HealthChecker():
    def __init__(self):
        port = int(os.environ.get("PORT", 5000))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        print(f"Escuchando UDP en el puerto {port}")

    def listen(self, alive_containers):
        while True:
            data, addr = self.sock.recvfrom(1024)
            node_id = data.decode().strip()
            alive_containers[node_id] = time.time()
            print(f"Recibido heartbeat de {node_id}")

    def check_health(self, alive_containers, timeout=20):
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
            time.sleep(5)

    def start(self):
        with Manager() as manager:
            alive_containers = manager.dict()
            p1 = Process(target=self.listen, args=(alive_containers,))
            p2 = Process(target=self.check_health, args=(alive_containers,))
            p1.start()
            p2.start()
            p1.join()
            p2.join()

if __name__ == "__main__":
    hc = HealthChecker()
    hc.start()

