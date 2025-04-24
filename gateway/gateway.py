#!/usr/bin/env python
import socket
import csv
import io
import ast
import json
from queue_manager.queue_manager import QueueManagerPublisher, QueueManagerConsumer
import constants
import time

EXCHANGE_METADATA = 'gateway_metadata'
EXCHANGE_CREDITS = 'gateway_credits'
EXCHANGE_RATINGS = 'gateway_ratings'

queue_manager_metadata = QueueManagerPublisher()
queue_manager_metadata.declare_exchange(EXCHANGE_METADATA, 'direct')

queue_manager_ratings = QueueManagerPublisher()  
queue_manager_ratings.declare_exchange(EXCHANGE_RATINGS, 'direct')

queue_manager_credits = QueueManagerPublisher()  
queue_manager_credits.declare_exchange(EXCHANGE_CREDITS, 'direct')


queue_manager_results = QueueManagerConsumer()
queue_manager_results.declare_exchange(exchange_name='results', exchange_type='direct')
queue_name = queue_manager_results.queue_declare(queue_name='')
queue_manager_results.queue_bind(
    exchange_name='results', queue_name=queue_name, routing_key='results')

eof_count = 0
EOF_WAITING = 6
END_OF_FILE = '<<EOF>>\n'

HOST = '0.0.0.0'
PORT = 5050

CLIENT_HOST = 'client'  
CLIENT_PORT = 5051  

MAX_BUFFER_SIZE = 4 * 1024 * 1024  # 4 MB

resultados = []

def process_movies_csv(csv_text, is_partial=False, residual_buffer=''):
    """
    Procesa un fragmento de texto CSV para movies_metadata.csv.
    is_partial: Indica si el texto es un fragmento parcial (no incluye EOF).
    residual_buffer: Línea incompleta del fragmento anterior.
    """
    # Combinar el texto nuevo con el residual
    csv_text = residual_buffer + csv_text
    f = io.StringIO(csv_text)
    reader = csv.reader(f)
    credits_sent = 0
    residual = ''
    
    # Si es parcial, la última línea podría estar incompleta
    if is_partial:
        lines = csv_text.splitlines()
        if lines and not lines[-1].endswith('\n'):
            residual = lines[-1]
            lines = lines[:-1]
            f = io.StringIO('\n'.join(lines))
            reader = csv.reader(f)

    for row in reader:
        if len(row) < 24:
            continue
        movie_id = row[5]
        budget = row[2]
        genres = row[3]
        overview = row[9]
        production_countries = row[13]
        release_date = row[14]
        revenue = row[15]
        title = row[20]
        if not all([movie_id, budget, genres, overview, production_countries, release_date, revenue, title]):
            continue
        row_str = f"{movie_id}{constants.SEPARATOR}{genres}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{overview}{constants.SEPARATOR}{production_countries}{constants.SEPARATOR}{release_date}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{title}"
        queue_manager_metadata.publish_message(
            exchange_name=EXCHANGE_METADATA,
            routing_key=str(movie_id[-1]),
            message=row_str
        )
        credits_sent += 1
        if credits_sent % 10000 == 0:
            print(f" [METADATA] Sent {credits_sent} messages")
    
    return residual

def process_ratings_csv(csv_text, is_partial=False, residual_buffer=''):
    """
    Procesa un fragmento de texto CSV para ratings.csv.
    is_partial: Indica si el texto es un fragmento parcial (no incluye EOF).
    residual_buffer: Línea incompleta del fragmento anterior.
    """
    csv_text = residual_buffer + csv_text
    f = io.StringIO(csv_text)
    credits_sent = 0
    reader = csv.reader(f)
    residual = ''
    
    # Saltar encabezado solo en el primer fragmento
    if not residual_buffer:
        next(reader, None)
    
    # Si es parcial, guardar la última línea si está incompleta
    if is_partial:
        lines = csv_text.splitlines()
        if lines and not lines[-1].endswith('\n'):
            residual = lines[-1]
            lines = lines[:-1]
            f = io.StringIO('\n'.join(lines))
            reader = csv.reader(f)

    for row in reader:
        if len(row) < 4:
            continue
        movie_id = row[1]
        rating = row[2]
        if not movie_id or not rating:
            continue
        row_str = f"{movie_id}{constants.SEPARATOR}{rating}"
        queue_manager_ratings.publish_message(
            exchange_name=EXCHANGE_RATINGS,
            routing_key=str(movie_id[-1]),
            message=row_str
        )
        credits_sent += 1
        if credits_sent % 1000000 == 0:
            print(f" [RATINGS] Sent {credits_sent} messages")
    
    return residual

def process_credits_csv(csv_text, is_partial=False, residual_buffer=''):
    csv_text = residual_buffer + csv_text
    residual = ''
    
    # Divide el texto en líneas
    lines = csv_text.splitlines(keepends=True)
    
    # Si el fragmento es parcial y la última línea no termina en salto de línea, se considera incompleta
    if is_partial and lines and not lines[-1].endswith('\n'):
        residual = lines.pop()  # Extrae la última línea incompleta
    
    # Si no hay líneas para procesar, retorna el residual
    if not lines:
        return residual
    
    # Crea un objeto StringIO con las líneas completas
    f = io.StringIO(''.join(lines))
    
    # Determina si se debe omitir la primera línea (encabezado)
    skip_header = not residual_buffer
    
    # Crea el lector CSV
    reader = csv.DictReader(f, fieldnames=['cast', 'crew', 'id'])
    
    # Omitir el encabezado si corresponde
    if skip_header:
        next(reader, None)
    
    credits_sent = 0
    
    for row in reader:
        try:
            cast = ast.literal_eval(row['cast'])
            movie_id = row['id']
            
            for actor in cast:
                row_str = f"{movie_id}{constants.SEPARATOR}{actor['id']}{constants.SEPARATOR}{actor['name']}"
                queue_manager_credits.publish_message(
                    exchange_name='gateway_credits',
                    routing_key=str(movie_id[-1]),
                    message=row_str
                )
                credits_sent += 1
                if credits_sent % 100000 == 0:
                    print(f" [CREDITS] Sent {credits_sent} messages")
        except Exception as e:
            print(f"Error processing row: {e}")
            continue
    
    return residual

def handle_client(conn):
    buffer = ''
    raw_buffer = b''
    current_type = None
    first_line = True
    content_buffer = ''
    residual_buffer = ''  # Para líneas incompletas entre fragmentos

    archivos_esperados = {'movies', 'ratings', 'credits'}
    archivos_procesados = set()

    while len(archivos_procesados) < len(archivos_esperados):
        chunk = conn.recv(65536)
        if not chunk:
            print("[!] Cliente cerró la conexión antes de enviar todos los archivos.")
            break
        raw_buffer += chunk
        #print(f"[DEBUG] Recibidos {len(chunk)} bytes, tamaño total de raw_buffer: {len(raw_buffer)}")

        while raw_buffer:
            try:
                # Intentar decodificar el buffer acumulado
                buffer += raw_buffer.decode('utf-8')
                raw_buffer = b''
                #print(f"[DEBUG] Buffer decodificado: {len(buffer)} caracteres")

                # Procesar el identificador si es la primera línea
                if first_line and '\n' in buffer:
                    lines = buffer.split('\n', 1)
                    current_type = lines[0].strip().lower()
                    buffer = lines[1] if len(lines) > 1 else ''
                    first_line = False
                    print(f"[DEBUG] Identificador detectado: {current_type}")

                # Acumular contenido en content_buffer
                content_buffer += buffer
                buffer = ''
                print(f"[DEBUG] Contenido acumulado: {len(content_buffer)} caracteres")

                # Procesar si el buffer excede el tamaño máximo o contiene EOF
                while len(content_buffer.encode('utf-8')) > MAX_BUFFER_SIZE or END_OF_FILE in content_buffer:
                    if END_OF_FILE in content_buffer:
                        content, content_buffer = content_buffer.split(END_OF_FILE, 1)
                        is_partial = False
                        print(f"[DEBUG] EOF detectado, procesando {len(content)} caracteres")
                    else:
                        content = content_buffer
                        content_buffer = ''
                        is_partial = True
                        print(f"[DEBUG] Buffer excede {MAX_BUFFER_SIZE} bytes, procesando {len(content)} caracteres")

                    # Procesar el contenido según el tipo
                    if current_type == 'movies':
                        print(f"[*] Procesando fragmento de 'movies'...")
                        residual_buffer = process_movies_csv(content, is_partial, residual_buffer)
                    elif current_type == 'ratings':
                        print(f"[*] Procesando fragmento de 'ratings'...")
                        residual_buffer = process_ratings_csv(content, is_partial, residual_buffer)
                    elif current_type == 'credits':
                        print(f"[*] Procesando fragmento de 'credits'...")
                        residual_buffer = process_credits_csv(content, is_partial, residual_buffer)
                    else:
                        print(f"[!] Identificador desconocido: {current_type}")
                        conn.sendall(f"ERROR: Identificador desconocido {current_type}\n".encode('utf-8'))
                        return

                    # Si se procesó el EOF, marcar el archivo como completo
                    if not is_partial:
                        archivos_procesados.add(current_type)
                        send_eof(current_type)
                        conn.sendall(f"OK: {current_type}\n".encode('utf-8'))
                        print(f"[DEBUG] Confirmación enviada: OK: {current_type}")
                        current_type = None
                        first_line = True
                        residual_buffer = ''
                        print("[DEBUG] Estado reiniciado para el siguiente archivo")

            except UnicodeDecodeError:
                print(" [!] Secuencia UTF-8 incompleta, esperando más datos...")
                break  # Esperar más datos



def send_eof(file_type):
    if 'movies' == file_type:
        print(" [x] Enviando EOF para metadata")
        for i in range(10):
            queue_manager_metadata.publish_message(
                exchange_name=EXCHANGE_METADATA, routing_key=str(i), message=constants.END)
        queue_manager_metadata.close_connection()

    if 'ratings'  == file_type:
        print(" [x] Enviando EOF para ratings")
        for i in range(10):
            queue_manager_ratings.publish_message(
                exchange_name=EXCHANGE_RATINGS, routing_key=str(i), message=constants.END)
        queue_manager_ratings.close_connection()

    if 'credits' == file_type:
        print(" [x] Enviando EOF para credits")
        for i in range(10):
            queue_manager_credits.publish_message(
                exchange_name=EXCHANGE_CREDITS, routing_key=str(i), message=constants.END)
        queue_manager_credits.close_connection()

def callback(ch, method, properties, body):
    global eof_count
    if body.decode() == constants.END:
        print(f" [*] Received {body.decode()} for all movies, exiting...")
        eof_count += 1
        if eof_count == EOF_WAITING:
            queue_manager_results.stop_consuming()
            queue_manager_results.close_connection()
            return
    if body.decode().startswith('Query'):
        print(f" [x] Processing result: {body.decode()}")
        resultados.append(body.decode())

def send_results_to_client():

    max_intentos = 5

    for intento in range(max_intentos):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                print(f"[*] Conectando al cliente en {CLIENT_HOST}:{CLIENT_PORT}...")
                s.connect((CLIENT_HOST, CLIENT_PORT))
                print("[*] Conectado al cliente, enviando resultados...")
                for linea in resultados:
                    s.sendall((linea + '\n').encode('utf-8'))
                s.sendall(constants.END_COMMUNICATION.encode('utf-8'))
                print("[*] Resultados enviados con éxito.")
                return
        except ConnectionRefusedError:
            print(f"[!] Conexión rechazada, intento {intento + 1}/{max_intentos}. Reintentando")
        except Exception as e:
            print(f"[!] Error enviando resultados: {e}")
            break
    print("[!] No se pudo conectar al cliente para enviar resultados tras varios intentos.")

if __name__ == '__main__':
    inicio = time.time()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permitir reutilizar el puerto
        server.bind((HOST, PORT))
        server.listen()
        print(f"[*] Gateway listening on {HOST}:{PORT}")

        # Primera conexión: recibir archivos
        conn, addr = server.accept()
        with conn:
            print(f"[+] Connected by {addr}")
            handle_client(conn)

        print("[*] All files received, waiting for results...")
        # Iniciar consumo de resultados
        queue_manager_results.consume_messages(queue_name, callback=callback)
        queue_manager_results.start_consuming()


    send_results_to_client()

    print("[*] Gateway shutting down...")
    print("[*] All files processed ", time.time() - inicio)