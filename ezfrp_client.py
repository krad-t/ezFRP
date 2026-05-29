from datetime import datetime
import socket
import struct
import threading
import selectors
from typing import cast


class Client:
    def __init__(self):
        self.config = self.configure()
        self.sockets = []
        self.sel = selectors.DefaultSelector()
        self.server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_control.connect((self.config['server_ip'], self.config['control_port']))
        self.sockets.append(self.server_control)
        self.control_thread = threading.Thread(target=self.handle_control, daemon=True)
        self.control_thread.start()


        self.sid2sock_map = dict()  # 维护一个socket映射表，key是来自服务端UDP包的session_id，value是本地的的socket对象实例
        self.sock2sid_map = dict() # 维护一个反向映射表，key是本地的socket对象实例，value是来自session_id

    def configure(self):
        import json
        try:
            with open('ezfrp_client.json', 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            print('ezfrp_client.json not found, creating a new one using default configuration')
            config = {
                "server_ip": "124.221.101.254",
                "control_port": 7000,
                "local_host": "127.0.0.1",
                "local_port": 25565,
                "udp_data_port":7001
            }
            with open('ezfrp_client.json', 'w') as f:
                f.write(json.dumps(config))
        return config

    def handle_control(self):
        print("Successfully connected to server", self.config['server_ip'])
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
                
                # threading.Thread(target=self.handle_control_udp, args=(client_sock,)).start()
                self.handle_control_udp(client_sock)
                break


    def handle_control_udp(self, client_socket: socket.socket):
        # udp
        def local2client(socket_in_map: socket.socket):
            while True:
                data_local, _ = socket_in_map.recvfrom(4096)
                data_local = struct.pack("!I", self.sock2sid_map[socket_in_map]) + data_local
                client_socket.sendto(data_local, (self.config['server_ip'], self.config['udp_data_port'])) # 转发给server的UDP数据端口

        def client2local():
            while True:
                client_data, _ = client_socket.recvfrom(4096)
                sid = struct.unpack("!I", client_data[:4])[0]
                print(f"client data: {client_data}")
                client_data = client_data[4:]
                if sid not in self.sid2sock_map: # 如果这个session_id之前没有连接过，那么就给他分配一个新的socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.bind(("127.0.0.1", 0)) # 绑定一个随机端口
                    self.sid2sock_map[sid] = sock
                    self.sock2sid_map[sock] = sid
                    threading.Thread(target=local2client, args=(sock,)).start() # 每个socket都要单独开一个线程来监听本地服务的响应数据，并转发给client
                
                sock = self.sid2sock_map[sid]
                sock.sendto(client_data, (self.config['local_host'], self.config['local_port'])) # 转发给本地服务
        
        print(f"client socket bound to {client_socket.getsockname()}")
        self.server_control.send(bytes(str(client_socket.getsockname()[1]).encode("utf-8"))) # 把这个随机端口发给server，让server知道往哪个端口发UDP数据
        # 但是如果Client处于NAT设备之后，那么这个端口不等于NAT设备映射出来的端口，所以还需要UDP打洞来获取正确的地址
        cmd = self.server_control.recv(1024).decode("utf-8") # 等待接收Server打洞的指令，这时候Client就可以发送一个UDP包过去，让Server判断收到的UDP包是否和通过server_control发过来的端口一致，从而确认Client的网络拓扑
        print(f"client command received: {cmd}")
        if cmd == "UDP_HOLE_PUNCHING":
            data = struct.pack("!I", 0)
            client_socket.sendto(data, (self.config['server_ip'], self.config['udp_data_port'])) # NAT 打洞
        threading.Thread(target=client2local).start()

    def handle_control_tcp(self):
        # tcp
        # 第一次是发送控制消息new，此消息表示"有新的外部用户连接了，client可以创建data socket了" 只接受一个new指令
        self.sel.register(self.server_control, selectors.EVENT_READ, data="NewUser")
        while True:
            events = self.sel.select()
            for key, mask in events:
                data = key.data
                if data == 'NewUser':
                    sock = cast(socket.socket, key.fileobj)
                    cmd = sock.recv(1024).decode('utf-8')
                    if cmd == 'new':
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}:{cmd}")
                        server_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        server_data.connect((self.config['server_ip'], self.config['control_port']))
                        self.sockets.append(server_data)
                        local_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        local_data.connect((self.config['local_host'], self.config['local_port']))
                        self.sockets.append(local_data)
                        self.sel.register(server_data, selectors.EVENT_READ, data=local_data)
                        self.sel.register(local_data, selectors.EVENT_READ, data=server_data)
                elif isinstance(data, socket.socket):
                    sock_a = cast(socket.socket, key.fileobj)
                    sock_b = key.data
                    try:
                        recv_data = sock_a.recv(1024)
                        if not recv_data:
                            self.sel.unregister(sock_a)
                            self.sel.unregister(sock_b)
                            sock_a.close()
                            sock_b.close()
                        else:
                            sock_b.send(recv_data)
                    except (ConnectionResetError, OSError):
                        self.sel.unregister(sock_a)
                        self.sel.unregister(sock_b)
                        sock_a.close()
                        sock_b.close()

    def send_cmd(self, param):
        pass

    def quit(self):
        for s in self.sockets:
            s.close()
        for s in self.sock2sid_map:
            s.close()

if __name__ == '__main__':
    client = Client()
    client.control_thread.join()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            break

