import socket
import threading

HOST = '192.168.1.160'
PORT = 5001

def receive(sock):
    while True:
        try:
            msg = sock.recv(1024).decode()
            if msg:
                print(f"\nServer: {msg}")
        except:
            print("Connection closed by server.")
            break

def send(sock):
    while True:
        msg = input("\nmsg for send to server: ")
        sock.send(msg.encode())

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((HOST, PORT))
print(f"Connected to server at {HOST}:{PORT}")

# Start threads
recv_thread = threading.Thread(target=receive, args=(client,))
send_thread = threading.Thread(target=send, args=(client,))
recv_thread.start()
send_thread.start()
