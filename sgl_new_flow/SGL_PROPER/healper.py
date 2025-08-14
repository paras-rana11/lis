import sys
import datetime
import serial  # type: ignore
import socket
import pymysql


PATH = 'C:\\ASTM\\root\\report_file\\'
PATH_CASE_FILE = 'C:\\ASTM\\root\\case_file\\'

# For TCP/IP Connection
SERVER_IP = "0.0.0.0"
SERVER_PORT = 5150


class MachineConnectionSerial:
    def __init__(self, connection_type: str, input_tty: str, machine_name: str):
        self.connection_type = connection_type
        self.input_tty = input_tty
        self.machine_name = machine_name
        self.port = None

    def get_filename(self):
        dt = datetime.datetime.now()
        return PATH + self.machine_name + dt.strftime("%Y-%m-%d-%H-%M-%S-%f")

    def get_port(self):
        try:
            self.port = serial.Serial(port=self.input_tty, baudrate=9600, timeout=1)
            print(f"Serial port {self.input_tty} opened successfully")
            return self.port, PATH_CASE_FILE
        except serial.SerialException as se:
            print(f"Failed to open serial port {self.input_tty}: {se}")
            sys.exit(1)

    def read(self):
        if not self.port:
            raise Exception("Port is not initialized. Call get_port() first.")
        data = self.port.read(1)
        # print(f"Received: {data}")
        return data

    def write(self, byte):
        if not self.port:
            raise Exception("Port is not initialized. Call get_port() first.")
        self.port.write(byte)
        # print(f"Query Sent: {byte}")

class MachineConnectionTcp:
    def __init__(self):
        self.connection = None
    
    def get_filename(self):
        dt = datetime.datetime.now()
        return PATH + "MACHINE_NAME" + dt.strftime("%Y-%m-%d-%H-%M-%S-%f")
    
    def get_connection(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((SERVER_IP, SERVER_PORT))
                s.listen()
                print(f"Server listening on 0.0.0.0:5150")
                connection, addr = s.accept()
                if connection:
                    return connection, PATH
        except serial.SerialException as se:
            print(f"Failed to open serial port COM4: {se}")
            sys.exit(1)

    def read(self):
        if not self.connection:
            raise Exception("connection is not initialized. Call get_connection() first.")
        data = self.connection.recv(1024)
        print(f"Received: {data}")
        return data
    
    def process_data(self, data):
        if data:
            return data.decode('utf-8')
    
    def write(self, byte):
        if not self.connection:
            raise Exception("connection is not initialized. Call get_connection() first.")
        self.connection.sendall(byte)
        print(f"ACK Sent: {byte}")

class GenerateChecksum:
    def __init__(self):
        # Initialize ASTM Protocol Constants
        self.STX = chr(0x02)  # Start of text
        self.ETX = chr(0x03)  # End of text
        self.EOT = chr(0x04)  # End of transmission
        self.ENQ = chr(0x05)  # Enquiry
        self.ACK = chr(0x06)  # Acknowledge
        self.NAK = chr(0x15)  # Negative Acknowledge
        self.CR = chr(0x0D)   # Carriage Return
        self.LF = chr(0x0A)   # Line Feed
        self.ETB = chr(0x17)

    def get_checksum_value(self, frame: str) -> str:
        """
        Reads checksum of an ASTM frame. Calculates characters after STX,
        up to and including the ETX or ETB.
        """
        sum_of_chars = 0
        complete = False

        # Loop through each character in the frame to calculate checksum
        for char in frame:
            byte_val = ord(char)

            if byte_val == 2:
                sum_of_chars = 0  
            elif byte_val in {3, 23}:
                sum_of_chars += byte_val
                complete = True
                break
            else:
                sum_of_chars += byte_val

        # Return the checksum if complete and valid
        if complete and sum_of_chars > 0:
            checksum = hex(sum_of_chars % 256)[2:].upper()  
            return checksum.zfill(2)  
        return "00" 

class ConnectionDb:
    def __init__(self , host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def connect_db(self):
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if conn:
                print("*******************************************")
                print("***********  DATABASE CONNECTED ***********")
                print("*******************************************")
                return conn
            else:
                return "Connectiona Not Established"
        except Exception as e:
            print(f"Connection Error : {e}")