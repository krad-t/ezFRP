import socket
"""
使用内置socket库完成echo服务器——直接返回客户端发送的任何消息
"""
PORT = 9000

s = socket.socket()
s.bind(('0.0.0.0', PORT))
s.listen(1)
print(f'server listening on {PORT}')
while True:
    conn, addr = s.accept()
    print(f'Connected by: {addr}')
    while True:
        data = conn.recv(1024)

        if len(data) != 0:
            conn.sendall(data)
        else:
            conn.close()
            break
    print(f'Disconnected by: {addr}')