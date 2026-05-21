import socket
from protocol import SERVER_IP,PUBLIC_PORT_UDP

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
while True:
    data = input(f"input anything(q to quit){s.getsockname()}:")
    if data == 'q':
        break
    else:
        s.sendto(bytes(data, 'utf-8'), (SERVER_IP, PUBLIC_PORT_UDP))
        recv_data, addr = s.recvfrom(1024)
        print(f'Received data from server {addr} : {recv_data.decode("utf-8")}')
s.close()
