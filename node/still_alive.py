import os
import socket
from multiprocessing import Process, Event
import random

class StillAlive:
    def __init__(self):
        self.port = int(os.environ.get("HEALTH_CHECK_PORT", 8000))
        self.hc_id = os.environ.get("HEALTH_CHECK_ID", "1")
        self.node_id = os.environ.get("CONTAINER_NAME", os.environ.get("HOSTNAME", "unknown"))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._stop_event = Event()
        self._process = None

    def _heartbeat_loop(self):
        try:
            while not self._stop_event.is_set():
                try:
                    self.sock.sendto(self.node_id.encode(), (f"health_checker_{self.hc_id}", self.port))
                    print(f"Enviado heartbeat de {self.node_id} al puerto {self.port} de health_checker_{self.hc_id}")
                except Exception as e:
                    print(f"Error enviando heartbeat: {e}")
                self._stop_event.wait(5)
        finally:
            self.close()

    def start(self):
        if self._process is None or not self._process.is_alive():
            self._process = Process(target=self._heartbeat_loop)
            self._process.daemon = True
            self._process.start()

    def stop(self):
        self._stop_event.set()
        if self._process is not None:
            self._process.join(timeout=2)

    def close(self):
        self.sock.close()
