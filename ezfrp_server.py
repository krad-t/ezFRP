import threading
import socket
import struct


class Server:
    def __init__(self):
        config = self.configure()
        self.sockets = []
        self.server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_control.bind(("0.0.0.0", config['control_port']))
        self.server_control.listen(1)
        self.sockets.append(self.server_control)
        self.server_client_udp_data = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_client_udp_data.bind(("0.0.0.0", config['udp_data_port']))
        self.sockets.append(self.server_client_udp_data)
        threading.Thread(target=self.handle_control, daemon=True).start()

        self.server_public_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_public_tcp.bind(("0.0.0.0", config['public_tcp_port']))
        self.server_public_tcp.listen(1)
        self.sockets.append(self.server_public_tcp)

        self.server_public_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_public_udp.bind(("0.0.0.0", config['public_udp_port']))
        self.sockets.append(self.server_public_udp)

        self.addr2sid_map = dict()  # 维护一个反向映射表，key是用户地址，value是session_id
        self.sid2addr_map = dict() # 维护一个会话映射表，key是session_id，value是用户地址
        self.session_sequence = 0  # 维护一个全局会话序列号，每当有新的用户连接进来时，就自增1，生成一个新的session_id

        print("Server initialized")

    def quit(self):
        for s in self.sockets:
            s.close()

    def configure(self):
        import json
        try:
            with open('ezfrp_server.json', 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            print('ezfrp_server.json not found, creating a new one using default configuration')
            config = {
                'control_port': 7000,
                'public_tcp_port': 9999,
                'public_udp_port': 9998,
                'udp_data_port': 7001
            }
            with open('ezfrp_server.json', 'w') as f:
                f.write(json.dumps(config))
        return config

    def handle_control(self):
        # 监听 :7000，处理 Client 的控制消息
        # control_channel 专门用来处理控制消息
        while True:
            control_channel, addr = self.server_control.accept()
            self.sockets.append(control_channel)
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
        def user2client(client_addr):
            while True:
                data_user, addr_user = self.server_public_udp.recvfrom(4096)
                print(f"Data [{data_user}] from {addr_user}")
                if addr_user not in self.addr2sid_map: # 如果这个用户之前没有连接过，那么就给他分配一个新的session_id
                    self.session_sequence += 1
                    session_id = self.session_sequence
                    self.addr2sid_map[addr_user] = session_id # 建立用户地址到session_id的映射
                else: # 如果这个用户之前连接过，那么就复用之前的session_id
                    session_id = self.addr2sid_map[addr_user]

                self.sid2addr_map[session_id] = addr_user # 建立session_id到用户地址的映射

                data_user = struct.pack("!I", session_id) + data_user
                self.server_client_udp_data.sendto(data_user, client_addr)

        def client2user():
            while True:
                data_client, _ = self.server_client_udp_data.recvfrom(4096)
                session_id = struct.unpack("!I", data_client[:4])[0]
                data_client = data_client[4:]
                if session_id != 0: # UDP 打洞的包，忽略
                    if session_id in self.sid2addr_map: # 如果这个session_id存在，那么就拿到对应的用户地址，转发给用户
                        user_addr = self.sid2addr_map[session_id]
                        self.server_public_udp.sendto(data_client, user_addr)

        client_port_udp = control_channel.recv(1024).decode('utf-8') # 等待client回复自己的udp地址
        client_addr_udp = (control_channel.getpeername()[0], int(client_port_udp)) # 拼出client的udp地址
        # 若client 处于 NAT 设备之后，则这个端口就是错误的，需要靠UDP打洞来获取正确的地址
        control_channel.send(bytes("UDP_HOLE_PUNCHING", "utf-8")) # 发送打洞指令给client，让client发送一个UDP包过来
        _, addr = self.server_client_udp_data.recvfrom(4096) # 从client发过来的UDP包中获取打洞后的端口
        client_port_udp_nat = addr[1] if addr[1] != client_addr_udp[1] else client_addr_udp[1]
        client_addr_udp = (control_channel.getpeername()[0], int(client_port_udp_nat))
        print(f"Client's UDP addr is                   {control_channel.getpeername()[0]}")
        print(f"Client's UDP port in LAN is            {client_port_udp}")
        print(f"Client's UDP port by mapping in NAT is {client_port_udp_nat}")
        print(f"The Client is after NAT: {client_port_udp != client_port_udp_nat}")

        # 双向转发
        threading.Thread(target=user2client, args=(client_addr_udp,)).start()
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
            self.sockets.append(conn_user)
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
            self.sockets.append(conn_client)
            print("Connected to client[data]", addr)

            # 再新开2个子线程做数据转发，每个子线程只负责单向转发，这样一旦有数据就会发走，就完成了双向转发
            threading.Thread(target=data_trans, args=(conn_user, conn_client)).start()
            threading.Thread(target=data_trans, args=(conn_client, conn_user)).start()

if __name__ == '__main__':
    server = Server()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            server.quit()
            break
