import socket
import threading

HOST = '192.168.1.160'
PORT = 5001

def get_actual_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Connect to a dummy address, doesn't have to be reachable
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        print("\n\n\n\nip: ", ip)
    finally:
        s.close()
        
    return ip

def receive(client_socket):
    while True:
        try:
            msg = client_socket.recv(1024).decode()
            if msg:
                print(f"\nClient: {msg}")
        except:
            print("Connection closed by client.")
            break

def send(client_socket):
    while True:
        msg = input("\nmsg for send to client: ")
        client_socket.send(msg.encode())

if HOST == get_actual_ip():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"Server listening on {HOST}:{PORT}...")

    client_socket, addr = server.accept()
    print(f"Connected to client at {addr}") #, client_socket={client_socket}")

    # Start receiving and sending threads
    recv_thread = threading.Thread(target=receive, args=(client_socket,))
    send_thread = threading.Thread(target=send, args=(client_socket,))
    recv_thread.start()
    send_thread.start()
