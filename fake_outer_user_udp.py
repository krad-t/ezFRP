import socket
import json


try:
    with open('ezfrp_client.json', 'r') as f:
        config_client = json.load(f)
except FileNotFoundError:
    print('json not found')

try:
    with open('ezfrp_server.json', 'r') as f:
        config_server = json.load(f)
except FileNotFoundError:
    print('json not found')

SERVER_IP = config_client['server_ip']
PUBLIC_PORT_UDP = config_server['public_udp_port']
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", 0)) # 绑定一个随机端口
while True:
    data = input(f"input anything(q to quit){s.getsockname()}:")
    if data == 'q':
        break
    else:
        s.sendto(bytes(data, 'utf-8'), (SERVER_IP, PUBLIC_PORT_UDP))
        recv_data, addr = s.recvfrom(1024)
        print(f'Received data from server {addr} : {recv_data.decode("utf-8")}')
s.close()
