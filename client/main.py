import socket
import os
import constants
import time
import signal
import sys

class FileTransferClient:
    def __init__(self):

        movies_file = os.getenv('MOVIES_FILE', 'movies.csv')
        ratings_file = os.getenv('RATINGS_FILE', 'ratings.csv')
        credits_file = os.getenv('CREDITS_FILE', 'credits.csv')


        self.GATEWAY_HOST = 'gateway'
        self.GATEWAY_PORT = 5050
        self.BUFFER_SIZE = 1024 * 1024
        self.CLIENT_LISTEN_PORT = 5051
        self.FILES_PATH = '/app/files'
        self.FILES = [
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

    def send_file(self, identifier, filename):
        """Send a file to the gateway."""
        path = os.path.join(self.FILES_PATH, filename)
        
        # Send logical identifier
        self.client_socket.sendall((identifier + '\n').encode('utf-8'))

        with open(path, 'r', encoding='utf-8') as f:
            buffer = ''
            for line in f:
                if len(buffer.encode('utf-8')) + len(line.encode('utf-8')) > self.BUFFER_SIZE:
                    print(f"[+] Sending {len(buffer)} bytes of data for file {filename}...")
                    self.client_socket.sendall(buffer.encode('utf-8'))
                    buffer = ''
                buffer += line
            if buffer:
                self.client_socket.sendall(buffer.encode('utf-8'))
            self.client_socket.sendall(constants.END_OF_FILE.encode('utf-8'))
            print(f"[+] File {filename} sent with identifier {identifier}.")
        
        confirmation = self.client_socket.recv(1024).decode('utf-8').strip()
        print(f"[*] Confirmation received: {confirmation}")

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
                line, buffer = buffer.split('\n', 1)
                if line in (constants.END_COMMUNICATION, constants.END_COMMUNICATION.strip()):
                    print("[*] Complete results received.")
                    return
                print(f"[RESULT] {line}")

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
        start = time.time()
        
        try:
            transfer_start = time.time()
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.GATEWAY_HOST, self.GATEWAY_PORT))
            print("[*] Connected to gateway.")
            
            for identifier, filename in self.FILES:
                self.send_file(identifier, filename)
            print("[*] All files sent. Time taken for transfer: {:.2f} seconds".format(time.time() - transfer_start))
            
            self.receive_results_inline()
            
        finally:
            self._cleanup_sockets()
        
        end = time.time()
        print(f"[*] Total execution time: {end - start:.2f} seconds or {(end - start) / 60:.2f} minutes.")

def main():
    client = FileTransferClient()
    client.run()

if __name__ == '__main__':
    main()