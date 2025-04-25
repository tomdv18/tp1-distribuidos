import socket
import os
import constants
import time

GATEWAY_HOST = 'gateway'  # nombre del servicio gateway en Docker Compose
GATEWAY_PORT = 5050        # puerto que usará el gateway para recibir
BUFFER_SIZE = 1024 * 1024  # tamaño del buffer para enviar datos
END_OF_FILE = '<<EOF>>\n'

CLIENT_LISTEN_PORT = 5051  # puerto que usará el cliente para recibir resultados   




ARCHIVOS_PATH = '/app/files'
ARCHIVOS = [
    ('movies', 'movies_metadata.csv'),
    ('ratings', 'ratings_60mb copy.csv'),
    ('credits', 'credits copy.csv')
]

def enviar_archivo(sock, identificador, archivo):
    ruta = os.path.join(ARCHIVOS_PATH, archivo)

    # Enviar identificador lógico
    sock.sendall((identificador + '\n').encode('utf-8'))

    with open(ruta, 'r', encoding='utf-8') as f:
        buffer = ''
        for linea in f:
            if len(buffer.encode('utf-8')) + len(linea.encode('utf-8')) > BUFFER_SIZE:
                print(f"[+] Enviando {len(buffer)} bytes de datos para el archivo {archivo}...")
                sock.sendall(buffer.encode('utf-8'))
                buffer = ''
            buffer += linea
        if buffer:
            sock.sendall(buffer.encode('utf-8'))
        sock.sendall(END_OF_FILE.encode('utf-8'))
        print(f"[+] Archivo {archivo} enviado con identificador {identificador}.")
    
    confirmacion = sock.recv(1024).decode('utf-8').strip()
    print(f"[*] Confirmación recibida: {confirmacion}")

def esperar_resultados():
    print(f"[*] Escuchando resultados en puerto {CLIENT_LISTEN_PORT}...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', CLIENT_LISTEN_PORT))
        server.listen()
        conn, addr = server.accept()
        with conn:
            print(f"[*] Conectado por el servidor {addr} para recibir resultados")
            buffer = ''
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    print("[!] Conexión cerrada.")
                    break
                buffer += chunk.decode('utf-8')
                while '\n' in buffer:
                    linea, buffer = buffer.split('\n', 1)
                    if linea == constants.END_COMMUNICATION or linea == constants.END_COMMUNICATION.strip():
                        print("[*] Resultados completos recibidos.")
                        return
                    print(f"[RESULTADO] {linea}")


def main():
    print("[*] Iniciando cliente...")
    inicio = time.time()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((GATEWAY_HOST, GATEWAY_PORT))
        print("[*] Conectado al gateway.")
        for identificador, archivo in ARCHIVOS:
            enviar_archivo(sock, identificador, archivo)
        print("[*] Todos los archivos enviados.")

    esperar_resultados()
    fin = time.time()
    print(f"[*] Tiempo total de ejecución: {fin - inicio:.2f} segundos. o {(fin - inicio) / 60:.2f} minutos.")


if __name__ == '__main__':
    main()