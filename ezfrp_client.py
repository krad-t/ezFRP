import socket
import threading
from protocol import *

control_channel: socket.socket = None

def handle_control():
    global control_channel
    # 线程 1：监听 :7000，处理 Server 的控制消息和发送来的数据
    server_control.connect((SERVER_IP, CONTROL_PORT))
    print("Connected to server", SERVER_IP)
    # 如果连接成功，第一次是发送控制消息，告诉client可以创建data socket了
    control_channel = server_control
    # 暂时不考虑保活、心跳等，只接受第一个new指令

    cmd = control_channel.recv(1024).decode('utf-8')
    if cmd == 'new':
        print(cmd)
        # 这个server_data是新建立出来的,用于数据转发，这里就是server里面拿到的那个conn_client抽象代表的“本尊”
        server_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_data.connect((SERVER_IP, CONTROL_PORT))
        threading.Thread(target=data_trans, args=(server_data, local_data)).start()
        threading.Thread(target=data_trans, args=(local_data, server_data)).start()


def data_trans(socketA: socket.socket, socketB: socket.socket):
    while True:
        data = socketA.recv(1024)
        socketB.send(data)


server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 这个是连接本地的软件（例如MC服务端）端口的
local_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
local_data.connect(('127.0.0.1', 25565))

t1 = threading.Thread(target=handle_control)
# t1.daemon = True
t1.start()
t1.join()
