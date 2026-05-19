import socket
from protocol import SERVER_IP

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((SERVER_IP,9999))
while True:
    data = input("input anything(q to quit):")
    if data == 'q':
        break
    else:
        s.send(bytes(data, 'utf-8'))
        recv_data = s.recv(1024)
        print(f'Received data from server {SERVER_IP} : {recv_data.decode("utf-8")}')
s.close()
