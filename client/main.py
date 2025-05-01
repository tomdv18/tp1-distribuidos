import socket
import os
import constants
import time
import signal
import sys

class FileTransferClient:
    def __init__(self):

        movies_file = os.getenv('ARCHIVO_MOVIES', 'movies.csv')
        ratings_file = os.getenv('ARCHIVO_RATINGS', 'ratings.csv')
        credits_file = os.getenv('ARCHIVO_CREDITS', 'credits.csv')


        self.GATEWAY_HOST = 'gateway'
        self.GATEWAY_PORT = 5050
        self.BUFFER_SIZE = 1024 * 1024
        self.CLIENT_LISTEN_PORT = 5051
        self.ARCHIVOS_PATH = '/app/files'
        self.ARCHIVOS = [
            ('movies', movies_file),
            ('ratings', ratings_file),
            ('credits', credits_file)
        ]
        self.client_socket = None
        self.server_socket = None
        self.connection_socket = None
        self._setup_signal_handler()

    def _setup_signal_handler(self):
        """Configure signal handler for graceful socket closure."""

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        print("\n[!] Interrupt received, closing sockets...")
        self._cleanup_sockets()
        sys.exit()
    
    def _cleanup_sockets(self):
        """Close all open sockets."""
        if self.client_socket:
            try:
                self.client_socket.close()
                print("[*] Client socket closed.")
            except:
                pass
        if self.connection_socket:
            try:
                self.connection_socket.close()
                print("[*] Connection socket closed.")
            except:
                pass
        if self.server_socket:
            try:
                self.server_socket.close()
                print("[*] Server socket closed.")
            except:
                pass

    def send_file(self, identificador, archivo):
        """Send a file to the gateway."""
        ruta = os.path.join(self.ARCHIVOS_PATH, archivo)
        
        # Send logical identifier
        self.client_socket.sendall((identificador + '\n').encode('utf-8'))

        with open(ruta, 'r', encoding='utf-8') as f:
            buffer = ''
            for linea in f:
                if len(buffer.encode('utf-8')) + len(linea.encode('utf-8')) > self.BUFFER_SIZE:
                    print(f"[+] Sending {len(buffer)} bytes of data for file {archivo}...")
                    self.client_socket.sendall(buffer.encode('utf-8'))
                    buffer = ''
                buffer += linea
            if buffer:
                self.client_socket.sendall(buffer.encode('utf-8'))
            self.client_socket.sendall(constants.END_OF_FILE.encode('utf-8'))
            print(f"[+] File {archivo} sent with identifier {identificador}.")
        
        confirmacion = self.client_socket.recv(1024).decode('utf-8').strip()
        print(f"[*] Confirmation received: {confirmacion}")

    def wait_for_results(self):
        """Listen for and process results from the server."""
        print(f"[*] Listening for results on port {self.CLIENT_LISTEN_PORT}...")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.CLIENT_LISTEN_PORT))
        self.server_socket.listen()
        
        self.connection_socket, addr = self.server_socket.accept()
        print(f"[*] Connected by server {addr} to receive results")
        
        buffer = ''
        while True:
            chunk = self.connection_socket.recv(4096)
            if not chunk:
                print("[!] Connection closed.")
                break
            buffer += chunk.decode('utf-8')
            while '\n' in buffer:
                linea, buffer = buffer.split('\n', 1)
                if linea in (constants.END_COMMUNICATION, constants.END_COMMUNICATION.strip()):
                    print("[*] Complete results received.")
                    return
                print(f"[RESULT] {linea}")

    def receive_results_inline(self):
        print("[*] Waiting for results...")
        buffer = ''
        while True:
            chunk = self.client_socket.recv(4096)
            if not chunk:
                print("[!] Gateway closed the connection.")
                break
            buffer += chunk.decode('utf-8')
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                if line.strip() == constants.END_COMMUNICATION.strip():
                    print("[*] Complete results received.")
                    return
                print(f"[RESULT] {line}")

    def run(self):
        """Main execution method."""
        print("[*] Starting client...")
        inicio = time.time()
        
        try:
            inicio_transferencia = time.time()
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.GATEWAY_HOST, self.GATEWAY_PORT))
            print("[*] Connected to gateway.")
            
            for identificador, archivo in self.ARCHIVOS:
                self.send_file(identificador, archivo)
            print("[*] All files sent. Time taken for transfer: {:.2f} seconds".format(time.time() - inicio_transferencia))
            
            self.receive_results_inline()
            
        finally:
            self._cleanup_sockets()
        
        fin = time.time()
        print(f"[*] Total execution time: {fin - inicio:.2f} seconds or {(fin - inicio) / 60:.2f} minutes.")

def main():
    client = FileTransferClient()
    client.run()

if __name__ == '__main__':
    main()