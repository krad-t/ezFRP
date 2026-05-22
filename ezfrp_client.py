from datetime import datetime
import socket
import struct
import threading

from protocol import SERVER_IP,CONTROL_PORT,UDP_DATA_PORT


class Client:
    def __init__(self):
        self.server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_control.connect((SERVER_IP, CONTROL_PORT))
        self.control_thread = threading.Thread(target=self.handle_control, daemon=True)
        self.control_thread.start()

        self.sid2sock_map = dict()  # 维护一个socket映射表，key是来自服务端UDP包的session_id，value是本地的的socket对象实例
        self.sock2sid_map = dict() # 维护一个反向映射表，key是本地的socket对象实例，value是来自session_id


    def handle_control(self):
        print("Successfully connected to server", SERVER_IP)
        while True:
            choice = input("Establishing TCP(1) or UDP(2) tunnel:")
            if choice == '1':
                self.server_control.send(bytes('TCP',"utf-8"))
                # daemon继承
                threading.Thread(target=self.handle_control_tcp).start()
                break
            elif choice == '2':
                self.server_control.send(bytes('UDP',"utf-8"))
                client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client_sock.bind(("0.0.0.0", 0)) # 绑定一个随机端口
                self.server_control.send(bytes(str(client_sock.getsockname()), "utf-8")) # 把这个随机端口发给server，让server知道往哪个端口发UDP数据
                # threading.Thread(target=self.handle_control_udp, args=(client_sock,)).start()
                self.handle_control_udp(client_sock)
                break


    def handle_control_udp(self, client_socket: socket.socket):
        # udp
        def local2client(socket_in_map: socket.socket):
            while True:
                data_local, _ = socket_in_map.recvfrom(4096)
                data_local = struct.pack("!I", self.sid2sock_map[socket_in_map]) + data_local
                client_socket.sendto(data_local, (SERVER_IP, UDP_DATA_PORT)) # 转发给server的UDP数据端口

        def client2local():
            while True:
                client_data, _ = client_socket.recvfrom(4096)
                sid = struct.unpack("!I", client_data[:4])[0]
                client_data = client_data[4:]
                if sid not in self.sid2sock_map: # 如果这个session_id之前没有连接过，那么就给他分配一个新的socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.bind(("127.0.0.1", 0)) # 绑定一个随机端口
                    self.sid2sock_map[sid] = sock
                    self.sock2sid_map[sock] = sid
                    threading.Thread(target=local2client, args=(sock,)).start() # 每个socket都要单独开一个线程来监听本地服务的响应数据，并转发给client
                
                sock = self.sid2sock_map[sid]
                sock.sendto(client_data, ("127.0.0.1", 25565)) # 转发给本地服务

        threading.Thread(target=client2local).start()

    def handle_control_tcp(self):
        # tcp
        # 第一次是发送控制消息new，此消息表示"有新的外部用户连接了，client可以创建data socket了"
        # 只接受一个new指令
        while True:
            cmd = self.server_control.recv(1024).decode('utf-8')
            if cmd == 'new':
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}:{cmd}")
                # 这个server_data是新建立出来的,用于数据转发
                server_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_data.connect((SERVER_IP, CONTROL_PORT))
                # 一对 [server_data --- local_data] ,local_data是本地的内存对象实例，server_data是另一侧Server端的抽象代表

                # 这个local_data是连接本地的软件（例如MC服务端）端口的，每个外部用户必须得配备一个新的Socket
                local_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                local_data.connect(('127.0.0.1', 25565))

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

                threading.Thread(target=data_trans, args=(server_data, local_data)).start()
                threading.Thread(target=data_trans, args=(local_data, server_data)).start()

    def send_cmd(self, param):
        pass


if __name__ == '__main__':
    client = Client()
    client.control_thread.join()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            break
