
import hashlib
import logging
import math
import socket
import threading
import numpy as np
import traceback
from _thread import *
import time
from datetime import datetime
from tqdm import tqdm

IP = socket.gethostbyname(socket.gethostname())
PORT = 4456
BUFFER_SIZE = 4096
ADDRESS = (IP, PORT)
Log_path = "data/Logs/"
FILE1 = 'data/Files/100MB.txt'
FILE2 = 'data/Files/250MB.txt'


HELLO = 'Hello'
READY = 'Ready'
NOMBRE = 'Nombre'
OK = 'Ok'
HASH = 'HashOk'
COMPLETE = 'Completado'
ERROR = 'Error'


def threadsafe_function(fn):
    """decorator making sure that the decorated function is thread safe"""
    lock = threading.Lock()

    def new(*args, **kwargs):
        lock.acquire()
        try:
            r = fn(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            lock.release()
        return r

    return new


class ServerProtocol:

    def __init__(self, numeroClientes, fileName):

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.thread_count = 0
        self.numeroClientes = numeroClientes
        self.fileName = fileName
        self.clientesReady = 0
        self.conexionesFallidas = 0
        self.allReady = threading.Event()
        self.tamañoArchivo = self.getTamañoArchivo()
        self.tiempoEjecucion = np.zeros(numeroClientes)
        self.conexionesCompletadas = np.zeros(numeroClientes)
        self.conexionesExitosas = np.zeros(numeroClientes)
        self.paquetesEnviados = np.zeros(numeroClientes)
        self.bytesEnviados = np.zeros(numeroClientes)

        try:
            self.server_socket.bind((IP, PORT))
        except socket.error as e:
            print(str(e))

        print('Esperando conexión..')
        self.server_socket.listen(5)

        now = datetime.now()

        dt_string = now.strftime("%Y-%d-%m-%H-%M-%S")

        logging.basicConfig(filename="data/Logs/{}.log".format(dt_string), level=logging.INFO)
        logging.info(dt_string)
        logging.info("Nombre del archivo: {}; Tamaño del archivo: {} ".format(self.fileName, self.tamañoArchivo))

    def enviarArchivoAlCliente(self, connection, thread_id):
        while True:
            try:
                reply = self.recibirDelCliente(connection)
                self.verificarRespuesta(reply, HELLO)

                self.enviarAlCliente(connection, HELLO, "El saludo del cliente {} fue recibido".format(thread_id),
                                    thread_id)

                self.enviarAlCliente(connection, f'{thread_id};{self.numeroClientes}',
                                    "Id enviado al cliente {}".format(thread_id),
                                    thread_id)

                reply = self.recibirDelCliente(connection)
                self.verificarRespuesta(reply, READY)

                self.updateClientesReady()

                while not self.allClientesReady(thread_id):
                    print('El cliente {} está en espera '.format(thread_id))
                    self.allReady.wait()

                self.enviarAlCliente(connection, self.fileName, "Enviando el nombre del archivo ({}) al cliente "
                                                                "{}".format(self.fileName, thread_id), thread_id)

                reply = self.recibirDelCliente(connection)

                self.verificarRespuesta(reply, NOMBRE)

                hash = self.hash_file()
                self.enviarAlCliente(connection, hash,
                                    "Enviando archivo hash ({}) al cliente {}".format(hash, thread_id),
                                    thread_id)
                reply = self.recibirDelCliente(connection)

                self.verificarRespuesta(reply, OK)

                size = str(self.tamañoArchivo)

                self.enviarAlCliente(connection, size,
                                    "Enviando tamaño del archivo ({}) al cliente {}".format(size, thread_id),
                                    thread_id)

                start_time = time.time()

                self.enviarArchivo(connection, thread_id)

                reply = self.recibirDelCliente(connection)

                self.tiempoEjecucion[thread_id - 1] = time.time() - start_time

                self.verificarRespuesta(reply, COMPLETE)

                reply = connection.recv(BUFFER_SIZE).decode('utf-8')
                self.verificarRespuesta(reply, HASH)

                print(" Integridad del archivo verificada por el cliente {}".format(thread_id))

                connection.close()
                self.conexionesCompletadas[thread_id - 1] = 1
                self.conexionesExitosas[thread_id - 1] = 1
                self.log_info()
                break

            except Exception as err:
                self.updateConexionesFallidas()
                connection.close()
                print("Error durante la transmisión del archivo al cliente {}: {} \n".format(thread_id, str(err)))
                self.conexionesCompletadas[thread_id - 1] = 1
                self.log_info()
                break

    def recibirDelCliente(self, connection):
        return connection.recv(BUFFER_SIZE).decode('utf-8')

    def enviarAlCliente(self, connection, segment, print_message, thread_id):
        b = connection.send(str.encode(segment))
        self.bytesEnviados[thread_id - 1] += int(b)

        print("\n", print_message)

    def verificarRespuesta(self, received, expected):
        if not expected == received:
            raise Exception("Error en el protocolo: esperaba {}; recibió {}".format(expected, received))

    @threadsafe_function
    def allClientesReady(self, thread_id):

        allReady = self.numeroClientes - self.clientesReady == 0

        if allReady:
            print("Todos los clientes están listos: empezando transporte {}".format(thread_id))

        return allReady

    def enviarArchivo(self, connection, thread_id):

        progress = tqdm(range(self.tamañoArchivo), f'Transferir al cliente{thread_id}', unit="B",
                        unit_scale=True,
                        unit_divisor=BUFFER_SIZE)

        with open( self.fileName, 'rb') as file:

            while True:
                chunk = file.read(BUFFER_SIZE)

                if chunk == b'':
                    break

                b = connection.send(chunk)
                self.bytesEnviados[thread_id - 1] += int(b)

                progress.update(len(chunk))

            self.paquetesEnviados[thread_id - 1] = self.tamañoArchivo / BUFFER_SIZE
            print("Transmisión al cliente {} completada".format(thread_id))
            file.close()

    def getTamañoArchivo(self):
        with open( self.fileName, 'rb') as file:
            packed_file = file.read()
        return int(len(packed_file))

    def hash_file(self):
        file = open(self.fileName, 'rb')
        h = hashlib.sha1()

     
        chunk = 0
        while chunk != b'':
 
            chunk = file.read(BUFFER_SIZE)
            h.update(chunk)
        file.close()

   
        return h.hexdigest()

    def close(self):
        self.server_socket.close()

    @threadsafe_function
    def updateClientesReady(self):
        self.clientesReady += 1

        if self.numeroClientes - self.clientesReady == 0:
            self.allReady.set()

    @threadsafe_function
    def updateConexionesFallidas(self):
        self.conexionesFallidas += 1

    def allCompleted(self):
        return self.conexionesCompletadas.sum() == self.numeroClientes

    def log_info(self):
        if self.allCompleted():
            logging.info(
                '_____________________________________________________________________________________________________')
            logging.info('Conexión exitosa:')
            d = {1: 'si', 0: 'no'}

            for n in range(self.numeroClientes):
                logging.info('Cliente{}: {}'.format(n + 1, d[self.conexionesExitosas[n]]))

            logging.info(
                '_____________________________________________________________________________________________________')
            logging.info('Tiempo de ejecución:')
            for n in range(self.numeroClientes):
                logging.info('Cliente{}: {} s'.format(n + 1, self.tiempoEjecucion[n]))

            logging.info(
                '_____________________________________________________________________________________________________')
            logging.info('Bytes enviados:')
            for n in range(self.numeroClientes):
                logging.info('Cliente{}: {} B'.format(n + 1, self.bytesEnviados[n]))

            logging.info(
                '_____________________________________________________________________________________________________')
            logging.info('Paquetes enviados:')
            for n in range(self.numeroClientes):
                logging.info('Cliente{}: {}'.format(n + 1, self.paquetesEnviados[n]))

    def run(self):

        while True:
            print('Escuchando en', self.server_socket.getsockname())

            Client, address = self.server_socket.accept()
            self.thread_count += 1

            print('Connectado a: ' + address[0] + ':' + str(address[1]))

            logging.info('Conexión al cliente {} ({}:{})'.format(self.thread_count, address[0], str(address[1])))

            if self.thread_count <= self.numeroClientes:
                start_new_thread(self.enviarArchivoAlCliente, (Client, self.thread_count))

            print('Thread Numero: ' + str(self.thread_count))


def main():
    fn = int(input("Escoge el archivo a enviar: \n1 100 MB \n2 250 MB  \n"))
    if fn==1:
        fileName=FILE1
    else:
        fileName=FILE2

    nc = int(input("Indicar número de clientes simultaneos: \n"))

    s = ServerProtocol(nc, fileName)
    s.run()


if __name__ == "__main__":
    main()
