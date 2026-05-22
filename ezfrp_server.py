import threading
import socket
from protocol import *


class Server:
    def __init__(self):
        self.server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_control.bind(("0.0.0.0", CONTROL_PORT))
        self.server_control.listen(1)
        self.server_client_udp_data = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_client_udp_data.bind(("0.0.0.0", UDP_DATA_PORT))

        threading.Thread(target=self.handle_control, daemon=True).start()

        self.server_public_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_public_tcp.bind(("0.0.0.0", PUBLIC_PORT_TCP))
        self.server_public_tcp.listen(1)

        self.server_public_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_public_udp.bind(("0.0.0.0", PUBLIC_PORT_UDP))

        self.session_map = dict()

        print("Server initialized")

    def handle_control(self):
        # 监听 :7000，处理 Client 的控制消息
        # control_channel 专门用来处理控制消息
        while True:
            control_channel, addr = self.server_control.accept()
            # todo:v0.6.0
            print(f"Connected by client[control]:{addr}")
            while True:
                try:
                    client_cmd = control_channel.recv(1024).decode('utf-8')
                    # 处理控制消息
                    print(f"CMD [{client_cmd}] from {addr}")
                    # daemon继承
                    if client_cmd == 'TCP':
                        threading.Thread(target=self.handle_public_tcp, args=(control_channel,)).start()
                    elif client_cmd == 'UDP':
                        # threading.Thread(target=self.handle_public_udp, args=(control_channel,)).start()
                        self.handle_public_udp(control_channel)
                except (ConnectionResetError, OSError):
                    print("Client disconnected, waiting for reconnect...")
                    break

    def handle_public_udp(self, control_channel: socket.socket):
        # 监听 :9998，处理外部用户连接
        def user2client():
            while True:
                data_user, addr_user = self.server_public_udp.recvfrom(4096)
                self.server_client_udp_data.sendto(data_user, addr_user)
                # self.session_map[udpid] = addr_user

        def client2user():
            while True:
                data_client, addr_client = self.server_client_udp_data.recvfrom(4096)
                self.server_public_udp.sendto(data_client, addr_client)
                # self.session_map[udpid] = addr_client

        # 双向转发
        threading.Thread(target=user2client).start()
        threading.Thread(target=client2user).start()


    def handle_public_tcp(self, control_channel: socket.socket):
        # 监听 :9999，处理外部用户连接
        def data_trans(socketA: socket.socket, socketB: socket.socket):
            try:
                while True:
                    data = socketA.recv(1024)
                    if not data:
                        socketB.close()
                        break
                    socketB.send(data)
            except (ConnectionResetError, OSError):
                pass
            finally:
                socketA.close()
                socketB.close()

        while True:
            conn_user, addr = self.server_public_tcp.accept()
            print("Connected to public[outerUser]", addr)
            # 此时新的用户连接进来了，我们才需要向client发送new指令，然后再拿到代表data socket的抽象代表
            if control_channel is not None:
                # todo: 把指令用类封装
                server_cmd = bytes('new', 'utf-8')
                control_channel.send(server_cmd)
            else:
                print("No client connected,wait for client...")
                continue
            # 拿新的data socket
            conn_client, addr = self.server_control.accept()  # 拿到代表client data传输的抽象代表
            print("Connected to client[data]", addr)

            # 再新开2个子线程做数据转发，每个子线程只负责单向转发，这样一旦有数据就会发走，就完成了双向转发
            threading.Thread(target=data_trans, args=(conn_user, conn_client)).start()
            threading.Thread(target=data_trans, args=(conn_client, conn_user)).start()


if __name__ == '__main__':
    server = Server()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            break
