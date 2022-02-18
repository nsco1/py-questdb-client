import socket

HOST = ''      # Standard loopback interface address (localhost)
PORT = 9009    # Port to listen on (non-privileged ports are > 1023)
SIZE = 1024    # Number of bytes to send / receive at one time

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            data = conn.recv(SIZE)
            if not data:
                break
            print(repr(data))