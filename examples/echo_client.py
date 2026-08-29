import socket
"""
使用内置socket库完成echo案例的客户端
"""
PORT = 9000
SERVER_IP = '127.0.0.1'
s = socket.socket()
print(f'client listening on {SERVER_IP}:{PORT}')
s.connect((SERVER_IP, PORT))
print(f'Connected to Server: {SERVER_IP}')

data = input("input anything")
s.send(bytes(data, 'utf-8'))

recv_data = s.recv(1024)
print(f'Received data from server {SERVER_IP} : {recv_data.decode("utf-8")}')
s.close()
print(f'Disconnected by: {SERVER_IP}')