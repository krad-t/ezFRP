import socket
import json


try:
    with open('../config/ezfrp_client.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print('json not found')

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
SERVER_IP = config['server_ip']
port = int(input("port:"))
s.connect((SERVER_IP,port))
while True:
    data = input(f"input anything(q to quit){s.getsockname()}:")
    if data == 'q':
        break
    else:
        s.send(bytes(data, 'utf-8'))
        recv_data = s.recv(1024)
        print(f'Received data from server {SERVER_IP} : {recv_data.decode("utf-8")}')
s.close()
