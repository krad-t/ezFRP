import threading
import socket

from protocol import *

control_channel: socket.socket = None

def handle_control():
    global control_channel
    # 线程 1：监听 :7000，处理 Client 控制消息
    server_control.bind(("0.0.0.0", CONTROL_PORT))
    server_control.listen(1)
    control_channel, addr = server_control.accept()
    print("Connected by client[control]", addr)
    # control_channel 专门用来处理控制消息
    while True:
        cmd = control_channel.recv(1024)
        # 处理控制消息
        print(cmd.decode('utf-8'))
        # 这里也要两个线程分别完成收消息和发消息？


def handle_public():
    # 线程 2：监听 :9999，处理外部用户连接
    server_public.bind(("0.0.0.0", PUBLIC_PORT))
    server_public.listen(1)
    while True:
        conn_user, addr = server_public.accept()
        print("Connected to public[outerUser]", addr)
        # 此时新的用户连接进来了，我们才需要向client发送new指令，然后再拿到代表data socket的抽象代表
        control_channel.send(bytes('new', 'utf-8'))
        # 拿新的data socket
        # control_channel.accept() 这个可以么？
        conn_client, addr = server_control.accept()  # 拿到代表client data传输的抽象代表
        print("Connected to client[data]", addr)
        # 再新开2个子线程做数据转发，每个子线程只负责单向转发，这样一旦有数据就会发走，就完成了双向转发
        threading.Thread(target=data_trans, args=(conn_user, conn_client)).start()
        threading.Thread(target=data_trans, args=(conn_client, conn_user)).start()


def data_trans(socketA: socket.socket, socketB: socket.socket):
    while True:
        data = socketA.recv(1024)
        socketB.send(data)


server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_public = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

t1 = threading.Thread(target=handle_control)
t2 = threading.Thread(target=handle_public)
t1.daemon = True
t2.daemon = True
t1.start()
t2.start()
t1.join()
t2.join()
