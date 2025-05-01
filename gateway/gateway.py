import socket
import os
import csv
import io
import ast
import time
from multiprocessing import Pool
import signal
import sys
from queue_manager.queue_manager import QueueManagerPublisher, QueueManagerConsumer
import constants

HOST = '0.0.0.0'
PORT = 5050
MAX_BUFFER_SIZE = 4 * 1024 * 1024
EOF_WAITING = int(os.getenv('EOF', '6'))


class CSVProcessor:
    def __init__(self, publisher, exchange, skip_header=True, log_interval=10000, end_marker=constants.END):
        self.publisher = publisher
        self.exchange = exchange
        self.skip_header = skip_header
        self.log_interval = log_interval
        self.end_marker = end_marker
        self.residual = ''
        self.count = 0
        self.closed = False

    def process(self, text,conn, partial=False):
        if self.closed:
            return ''

        text = self.residual + text
        self.residual = ''
        lines = text.splitlines(keepends=True)

        if partial and lines and not lines[-1].endswith('\n'):
            self.residual = lines.pop()

        if not lines:
            return self.residual

        buffer = io.StringIO(''.join(lines))

        if self.skip_header:
            next(buffer, None)
            self.skip_header = False

        reader = csv.reader(buffer)

        for row in reader:
            if not row or any(not cell for cell in row[:1]):
                continue
            msg, key = self._format_and_key(row, conn)
            if not msg:
                continue
            self._publish(key, msg)

        return self.residual

    def _format_and_key(self, row, conn):
        if self.exchange == 'gateway_metadata':
            if len(row) < 24:
                return None, None
            movie_id, genres, budget, overview, countries, date, revenue, title = (
                row[5], row[3], row[2], row[9], row[13], row[14], row[15], row[20]
            )
            if not all([movie_id, genres, budget, overview, countries, date, revenue, title]):
                return None, None
            msg = f"{movie_id}{constants.SEPARATOR}{genres}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{overview}{constants.SEPARATOR}{countries}{constants.SEPARATOR}{date}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{title}{constants.SEPARATOR}{conn}"
        elif self.exchange == 'gateway_ratings':
            if len(row) < 3 or not row[1] or not row[2]:
                return None, None
            msg = f"{row[1]}{constants.SEPARATOR}{row[2]}{constants.SEPARATOR}{conn}"
            movie_id = row[1]
        elif self.exchange == 'gateway_credits':
            return None, None
        else:
            return None, None

        key = movie_id[-1]
        return msg, key

    def _publish(self, routing_key, message):
        self.publisher.publish_message(exchange_name=self.exchange, routing_key=str(routing_key), message=message)
        self.count += 1
        if self.count % self.log_interval == 0:
            print(f" [{self.exchange.upper()}] sent {self.count}")

    def send_end(self):
        for i in range(10):
            self.publisher.publish_message(exchange_name=self.exchange, routing_key=str(i), message=self.end_marker)
        self.publisher.close_connection()
        self.closed = True
        print(f" [x] EOF sent for {self.exchange}")


class CreditsProcessor(CSVProcessor):
    def process(self, text,conn, partial=False):
        if self.closed:
            return ''

        text = self.residual + text
        self.residual = ''
        lines = text.splitlines(keepends=True)

        if partial and lines and not lines[-1].endswith('\n'):
            self.residual = lines.pop()

        if not lines:
            return self.residual

        buffer = io.StringIO(''.join(lines))
        reader = csv.DictReader(buffer, fieldnames=['cast', 'crew', 'id'])

        if self.skip_header:
            next(reader, None)
            self.skip_header = False

        for row in reader:
            try:
                cast_list = ast.literal_eval(row['cast'])
                movie_id = row['id']
                for actor in cast_list:
                    msg = f"{movie_id}{constants.SEPARATOR}{actor['id']}{constants.SEPARATOR}{actor['name']}{constants.SEPARATOR}{conn}"
                    key = movie_id[-1]
                    self._publish(key, msg)
            except Exception:
                continue

        return self.residual


class Gateway:
    def __init__(self):
        self.meta_proc = CSVProcessor(QueueManagerPublisher(), 'gateway_metadata')
        self.meta_proc.publisher.declare_exchange('gateway_metadata', 'direct')
        self.rate_proc = CSVProcessor(QueueManagerPublisher(), 'gateway_ratings', log_interval=1000000)
        self.rate_proc.publisher.declare_exchange('gateway_ratings', 'direct')
        self.cred_proc = CreditsProcessor(QueueManagerPublisher(), 'gateway_credits')
        self.cred_proc.publisher.declare_exchange('gateway_credits', 'direct')

        self.results = []
        self.eof_count = 0
        self.consumer = QueueManagerConsumer()
        self.consumer.declare_exchange(exchange_name='results', exchange_type='direct')
        self.queue = self.consumer.queue_declare(queue_name='')
        self.consumer.queue_bind(exchange_name='results', queue_name=self.queue, routing_key='results')

    def handle_client(self, conn):
        content_buffer = ''
        buffer = ''
        raw = b''
        current = None
        first = True
        residual = ''
        expected = {'movies', 'ratings', 'credits'}
        done = set()

        while len(done) < len(expected):
            chunk = conn.recv(65536)
            if not chunk:
                print(" [!] client closed prematurely")
                break
            raw += chunk

            try:
                buffer += raw.decode('utf-8')
                raw = b''

                if first and '\n' in buffer:
                    current, rest = buffer.split('\n', 1)
                    current = current.strip().lower()
                    buffer = rest
                    first = False
                    print(f"[*] Detected file: {current}")

                content_buffer += buffer
                buffer = ''

                while len(content_buffer.encode('utf-8')) > MAX_BUFFER_SIZE or constants.END_OF_FILE in content_buffer:
                    if constants.END_OF_FILE in content_buffer:
                        part, content_buffer = content_buffer.split(constants.END_OF_FILE, 1)
                        partial = False
                        print(f"[*] Processing {current} chunk")
                    else:
                        part = content_buffer
                        content_buffer = ''
                        partial = True
                        print(f"[*] Processing {current} chunk")

                    if current == 'movies':
                        residual = self.meta_proc.process(part, partial,conn)
                    elif current == 'ratings':
                        residual = self.rate_proc.process(part, partial,conn)
                    elif current == 'credits':
                        residual = self.cred_proc.process(part, partial,conn)
                    else:
                        conn.sendall(f"ERROR: unknown {current}\n".encode('utf-8'))
                        return

                    if not partial:
                        done.add(current)
                        if current == 'movies':
                            self.meta_proc.send_end()
                        elif current == 'ratings':
                            self.rate_proc.send_end()
                        elif current == 'credits':
                            self.cred_proc.send_end()
                        conn.sendall(f"OK: {current}\n".encode('utf-8'))
                        current = None
                        first = True
                        residual = ''
                        content_buffer = ''
                        print("[*] State reset for next file")

            except UnicodeDecodeError:
                print(" [!] incomplete UTF-8, waiting")
                continue

    def collect_results(self):
        def cb(ch, method, props, body):
            msg = body.decode()
            if msg == constants.END:
                self.eof_count += 1
                if self.eof_count == EOF_WAITING:
                    self.consumer.stop_consuming()
                    self.consumer.close_connection()
                return
            if msg.startswith('Query'):
                print(f"[*] Result: {msg}")
                self.results.append(msg)

        self.consumer.consume_messages(self.queue, callback=cb)
        self.consumer.start_consuming()

    def send_results(self, conn):
        try:
            print("[*] Sending results to client")
            for line in self.results:
                conn.sendall((line + '\n').encode('utf-8'))
            conn.sendall(constants.END_COMMUNICATION.encode('utf-8'))
            print("[*] Results sent")
        except Exception as e:
            print(f"[!] Failed to send results: {e}")


def handle_client_wrapper(args):
    conn, addr = args
    gateway = Gateway()
    with conn:
        print(f"[+] Connection from {addr}")
        gateway.handle_client(conn)
        gateway.collect_results()
        gateway.send_results(conn)


def run_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, PORT))
        srv.listen()
        print(f"[*] Listening on {HOST}:{PORT}")

        with Pool(processes=4) as pool:  # Cambia el número de procesos según tu CPU
            try:
                while True:
                    conn, addr = srv.accept()
                    print(f"[+] Accepted connection from {addr}")
                    pool.apply_async(handle_client_wrapper, args=((conn, addr),))
            except KeyboardInterrupt:
                print("\n[!] Server interrupted, shutting down...")
                srv.close()


if __name__ == '__main__':
    run_server()
