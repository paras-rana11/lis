
import socket

MACHINE_ID = 'BS_240' 
HOST = '192.168.1.160' 
PORT = 5050


class ConnectionTCP_IP():
    def __init__(self, host, port, machine_name):
        self.connection = None
        self.host = host
        self.port = port
        self.is_conected = False
        
        
    def connect_to_server(self):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.host, self.port))

            print(f"Client connected to server on {self.host} : {self.port}")
            return client_socket
            
        except Exception as e:
            print(e)
        


connection = ConnectionTCP_IP(HOST, PORT, MACHINE_ID)

client_con = connection.connect_to_server()
     
while True:
    try:
        msg = input("Enter Byte to Send: ")
    
        byte_msg = msg.encode('utf-8')

        if client_con:
            client_con.sendall(byte_msg)
            
        else:
            pass
    
    except Exception as e:
            print(e)
        