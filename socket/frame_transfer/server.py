import os
import socket
from datetime import datetime

MACHINE_ID = 'BS_240'
SERVER_HOST_ADDRESS = '192.168.1.160'
SERVER_HOST_PORT = 5050  

LOG_FOLDER = 'C:\\All\\LIS\\socket'


class ConnectionTCP_IP:
    def __init__(self, server_ip, server_port, machine_name):
        self.connection = None
        self.server_ip = server_ip
        self.server_port = int(server_port)
        self.machine_name = machine_name
        self.is_connected = False
        self.server_socket = None

    def get_conn(self):
        if self.is_connected and self.connection:
            print("\nUsing Existing Connection")
            return self.connection, self.connection.getpeername()

        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(1)
            
            print(f"Waiting for connection on {self.server_ip}:{self.server_port}...")
            self.connection, addr = self.server_socket.accept()
            self.connection.settimeout(1.0)
            self.is_connected = True
            print(f"Server Connected To {addr}")
            return self.connection, addr

        except Exception as e:
            self.close_connection()
            print(f"Error while establishing connection - {e}")
            return None, None

    def read(self):
        if not self.connection:
            raise Exception("Not connected to server. First establish a connection.")

        try:
            data = self.connection.recv(1)
            return data
        except socket.timeout:
            return b''
        except Exception as e:
            print(f"Error reading data: {e}")
            return b''

    def write(self, byte_data):
        if not self.connection:
            raise Exception("Not connected to server. First establish a connection.")

        try:
            self.connection.sendall(byte_data)
            print(f"Data sent: {byte_data}")
        except Exception as e:
            print(f"Error sending data: {e}")

    def get_filename(self):
        dt = datetime.now()
        filename = f"{self.machine_name}_{dt.strftime('%Y-%m-%d-%H-%M-%S-%f')}.log"
        return os.path.join(LOG_FOLDER, filename)

    def close_connection(self):
        if self.connection:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
            except:
                pass
            self.connection = None

        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
            self.server_socket = None

        self.is_connected = False


connection = ConnectionTCP_IP(
    server_ip=SERVER_HOST_ADDRESS,
    server_port=SERVER_HOST_PORT,
    machine_name=MACHINE_ID
)

server_con = connection.get_conn()

logfile_path = connection.get_filename()

with open(logfile_path, 'ab') as log:
    while True:
        try:
            byte = connection.read()
            if byte:
                print(f"Received: {byte}")
                log.write(byte)
                log.flush()
            else:
                print("Waiting...")
        except Exception as e:
            print(f"Error: {e}")
            connection.close_connection()
            connection.get_conn()



