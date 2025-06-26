import socket
import os
import csv
import io
import ast
from multiprocessing import Pool
import uuid
from queue_manager.queue_manager import QueueManagerPublisher, QueueManagerConsumer
import constants

HOST = '0.0.0.0'
PORT = 5050
MAX_BUFFER_SIZE = 4 * 1024 * 1024
EOF_WAITING = int(os.getenv('EOF', '6'))

def log(msg):
    print(f"[PID {os.getpid()}] {msg}")

class CSVProcessor:
    def __init__(self, publisher, exchange, id_generator, skip_header=True, log_interval=10000, end_marker=constants.END):
        self.publisher = publisher
        self.exchange = exchange
        self.skip_header = skip_header
        self.log_interval = log_interval
        self.end_marker = end_marker
        self.residual = ''
        self.count = 0
        self.closed = False
        self.generate_message_id = id_generator


    def send_timeout(self, addr):
        for i in range(10):
            self.publisher.publish_message(exchange_name=self.exchange, routing_key=str(i), message=f"{constants.CLIENT_TIMEOUT} {addr}")
        self.publisher.close_connection()
        self.closed = True
        log(f"[x] TIMEOUT sent for client {addr}")    

    def process(self, text, addr, partial=False):
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
            msg, key = self._format_and_key(row, addr)
            if not msg:
                continue
            self._publish(key, msg)

        return self.residual

    def _format_and_key(self, row, addr):
        if self.exchange == 'gateway_metadata':
            if len(row) < 24:
                return None, None
            movie_id, genres, budget, overview, countries, date, revenue, title = (
                row[5], row[3], row[2], row[9], row[13], row[14], row[15], row[20]
            )
            if not all([movie_id, genres, budget, overview, countries, date, revenue, title]):
                return None, None
            
            msg = f"{movie_id}{constants.SEPARATOR}{genres}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{overview}{constants.SEPARATOR}{countries}{constants.SEPARATOR}{date}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{title}{constants.SEPARATOR}{addr}{constants.SEPARATOR}{self.generate_message_id(self.exchange)}{constants.SEPARATOR}1"

        elif self.exchange == 'gateway_ratings':
            if len(row) < 3 or not row[1] or not row[2]:
                return None, None
            msg = f"{row[1]}{constants.SEPARATOR}{row[2]}{constants.SEPARATOR}{addr}{constants.SEPARATOR}{self.generate_message_id(self.exchange)}{constants.SEPARATOR}1"


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
            log(f"[{self.exchange.upper()}] sent {self.count}")

    def send_end(self, addr):
        for i in range(10):
            self.publisher.publish_message(exchange_name=self.exchange, routing_key=str(i), message=f"{self.end_marker} {addr}")
        self.publisher.close_connection()
        self.closed = True
        log(f"[x] EOF sent for {self.exchange} for client {addr}")


class CreditsProcessor(CSVProcessor):
    def process(self, text, addr, partial=False):
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
                    msg = f"{movie_id}{constants.SEPARATOR}{actor['id']}{constants.SEPARATOR}{actor['name']}{constants.SEPARATOR}{addr}{constants.SEPARATOR}{self.generate_message_id(self.exchange)}{constants.SEPARATOR}1"
        

                    key = movie_id[-1]
                    self._publish(key, msg)
            except Exception:
                continue

        return self.residual


class Gateway:
    def __init__(self, client_id):

        self.message_counters = { # Cuenta los mensajes enviados por exchange
            'gateway_metadata': 0,
            'gateway_ratings': 0,
            'gateway_credits': 0
        }

        self.meta_proc = CSVProcessor(QueueManagerPublisher(), 'gateway_metadata',id_generator=self.generate_message_id)
        self.meta_proc.publisher.declare_exchange('gateway_metadata', 'direct')
        self.rate_proc = CSVProcessor(QueueManagerPublisher(), 'gateway_ratings', id_generator=self.generate_message_id, log_interval=1000000)
        self.rate_proc.publisher.declare_exchange('gateway_ratings', 'direct')
        self.cred_proc = CreditsProcessor(QueueManagerPublisher(), 'gateway_credits', id_generator=self.generate_message_id)
        self.cred_proc.publisher.declare_exchange('gateway_credits', 'direct')

        self.results = {}
        self.eof_count = 0
        self.client_finished = 0
        self.consumer = QueueManagerConsumer()
        self.consumer.declare_exchange(exchange_name='results', exchange_type='direct')
        self.queue = self.consumer.queue_declare(queue_name=f'results_{client_id}')
        self.consumer.queue_bind(exchange_name='results', queue_name=self.queue, routing_key='results')
        self.clients_timeout = []

    def client_timeout(self, current, conn, addr):
        log(f"[*] Client {addr} timed out, sending timeout message")

        if current is None:
            log(f"[!] Client {addr} timed out with no current file.")
            return
        
        self.meta_proc.send_timeout(addr)
        
        self.rate_proc.send_timeout(addr)
        
        self.cred_proc.send_timeout(addr)

        self.clients_timeout.append(addr)

    def generate_message_id(self, exchange):
        prefix = {
            'gateway_metadata': constants.MOVIES_PREFIX,
            'gateway_ratings': constants.RATINGS_PREFIX,
            'gateway_credits': constants.CREDITS_PREFIX
        }.get(exchange, 'X')

        counter = self.message_counters[exchange]
        self.message_counters[exchange] += 1
        return counter


    def handle_client(self, conn, addr):
        content_buffer = ''
        buffer = ''
        raw = b''
        current = None
        first = True
        residual = ''
        expected = {'movies', 'ratings', 'credits'}
        done = set()

        while len(done) < len(expected):
            try:
                chunk = conn.recv(65536)
                if not chunk:
                    log(" [!] client closed prematurely")
                    self.client_timeout(current, conn, addr)
                    break
                raw += chunk
            except socket.timeout:
                log(f"[!] Timeout: no data received from {addr}")
                self.client_timeout(current, conn, addr)
                break


            try:
                buffer += raw.decode('utf-8')
                raw = b''

                if first and '\n' in buffer:
                    current, rest = buffer.split('\n', 1)
                    current = current.strip().lower()
                    buffer = rest
                    first = False
                    log(f"[*] Detected file: {current}")

                content_buffer += buffer
                buffer = ''

                while len(content_buffer.encode('utf-8')) > MAX_BUFFER_SIZE or constants.END_OF_FILE in content_buffer:
                    if constants.END_OF_FILE in content_buffer:
                        part, content_buffer = content_buffer.split(constants.END_OF_FILE, 1)
                        partial = False
                        log(f"[*] Processing {current} chunk")
                    else:
                        part = content_buffer
                        content_buffer = ''
                        partial = True
                        log(f"[*] Processing {current} chunk")

                    if current == 'movies':
                        residual = self.meta_proc.process(part, addr, partial)
                    elif current == 'ratings':
                        residual = self.rate_proc.process(part, addr, partial)
                    elif current == 'credits':
                        residual = self.cred_proc.process(part, addr, partial)
                    else:
                        conn.sendall(f"ERROR: unknown {current}\n".encode('utf-8'))
                        return

                    if not partial:
                        done.add(current)
                        if current == 'movies':
                            self.meta_proc.send_end(addr)
                        elif current == 'ratings':
                            self.rate_proc.send_end(addr)
                        elif current == 'credits':
                            self.cred_proc.send_end(addr)
                        conn.sendall(f"OK: {current}\n".encode('utf-8'))
                        current = None
                        first = True
                        residual = ''
                        content_buffer = ''
                        log("[*] State reset for next file")

            except UnicodeDecodeError:
                log(" [!] incomplete UTF-8, waiting")
                continue

    def collect_results(self, conn, addr):
        try:
            def cb(ch, method, props, body):
                if addr in self.clients_timeout:
                    log(f"[!] Client {addr} has timed out, ignoring results")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                msg = body.decode()
                if msg.startswith(constants.END):
                    client = msg[len(constants.END):].strip()
                    print(f"client: {client}, addr: {addr}")
                    if client == str(addr):
                        self.client_finished += 1
                        log(f"[*] Client {addr} send finished {self.client_finished} times. Expected {EOF_WAITING}")
                        if self.client_finished == EOF_WAITING:
                            log(f"[*] Received EOF for client {addr}")
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            self.consumer.stop_consuming()
                            self.consumer.close_connection()
                            return

                if msg.startswith('Query'):
                    msg = msg.split(constants.SEPARATOR)
                    print(f"[*] Received query result: {msg}")
                    if msg[1] == str(addr):
                        if addr not in self.results:
                            self.results[addr] = []
                        if msg[0] not in self.results[addr]:
                            self.results[addr].append(msg[0])

                ch.basic_ack(delivery_tag=method.delivery_tag)

            self.consumer.consume_messages(self.queue, callback=cb)
            self.consumer.start_consuming()

        except Exception as e:
            log(f"[!] Error while collecting results: {e}")
            if addr in self.results:
                del self.results[addr] 
                log(f"[!] Cleared results for {addr} due to error")
            self.consumer.stop_consuming()
            self.consumer.close_connection()

    def send_results(self, conn, addr):
        try:
            log("[*] Sending results to client")
            for line in self.results[addr]:
                conn.sendall((line + '\n').encode('utf-8'))
            conn.sendall(constants.END_COMMUNICATION.encode('utf-8'))
            log("[*] Results sent")
        except Exception as e:
            log(f"[!] Failed to send results: {e}")
            if addr in self.results:
                del self.results[addr] 
                log(f"[!] Cleared results for {addr} due to send failure")


def handle_client_wrapper(args):
    conn, addr = args
    client_id = uuid.uuid4()
    gateway = Gateway(client_id)
    with conn:
        log(f"[+] Connection from {addr} with id {client_id}")
        conn.settimeout(120)        
        gateway.handle_client(conn, client_id)
        log(f"[*] Finished processing client {client_id}")
        conn.settimeout(900)  
        gateway.collect_results(conn, client_id)
        log(f"[*] Finished collecting results for client {client_id}")
        conn.settimeout(600)
        gateway.send_results(conn, client_id)
        log(f"[*] Finished sending results for client {client_id}")


def run_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, PORT))
        srv.listen()
        log(f"[*] Listening on {HOST}:{PORT}")

        with Pool(processes=10) as pool:
            try:
                while True:
                    conn, addr = srv.accept()
                    log(f"[+] Accepted connection from {addr}")
                    pool.apply_async(handle_client_wrapper, args=((conn, addr),))
            except KeyboardInterrupt:
                log("\n[!] Server interrupted, shutting down...")
                srv.close()


if __name__ == '__main__':
    run_server()
