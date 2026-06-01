import threading
import socket
import struct
import selectors
from typing import cast


class Server:
    def __init__(self):
        config = Server.configure()
        self.tcp_sel = selectors.DefaultSelector()
        self.udp_sel = selectors.DefaultSelector()
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
        self.sid2addr_map = dict()  # 维护一个会话映射表，key是session_id，value是用户地址
        self.session_sequence = 0  # 维护一个全局会话序列号，每当有新的用户连接进来时，就自增1，生成一个新的session_id

        print("Server initialized")

    def quit(self):
        for s in self.sockets:
            s.close()

    @staticmethod
    def configure():
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

    @staticmethod
    def pack_udp(sid: int, data: bytes, method="!I") -> bytes:
        return struct.pack(method, sid) + data

    @staticmethod
    def unpack_udp(data: bytes, method="!I") -> (int, bytes):
        session_id = struct.unpack(method, data[:4])[0]
        packet_data = data[4:]
        return session_id, packet_data

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
                        # threading.Thread(target=self.handle_public_tcp, args=(control_channel,)).start()
                        self.handle_public_tcp(control_channel)
                    elif client_cmd == 'UDP':
                        # threading.Thread(target=self.handle_public_udp, args=(control_channel,)).start()
                        self.handle_public_udp(control_channel)
                except (ConnectionResetError, OSError):
                    print("Client disconnected, waiting for reconnect...")
                    break

    def handle_public_udp(self, control_channel: socket.socket):
        # 若client 处于 NAT 设备之后，则这个端口就是错误的，需要靠UDP打洞来获取正确的地址
        control_channel.send(bytes("UDP_HOLE_PUNCHING", "utf-8"))  # 发送打洞指令给client，让client发送一个UDP包过来
        _, client_port_udp_nat = self.server_client_udp_data.recvfrom(4096)  # 从client发过来的UDP包中获取打洞后的端口
        client_port_udp_nat = client_port_udp_nat[1]
        client_addr_udp = (control_channel.getpeername()[0], int(client_port_udp_nat))
        print(f"Client's UDP addr is                   {control_channel.getpeername()[0]}")
        print(f"Client's UDP port by mapping in NAT is {client_port_udp_nat}")

        self.udp_sel.register(self.server_public_udp, selectors.EVENT_READ, data="->client")
        self.udp_sel.register(self.server_client_udp_data, selectors.EVENT_READ, data="->user")

        while True:
            events = self.udp_sel.select()
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    origin_sock = cast(socket.socket, key.fileobj)
                    packet_type = key.data
                    if packet_type == "->client":
                        packet_data, addr = origin_sock.recvfrom(4096)
                        if addr not in self.addr2sid_map:
                            self.session_sequence += 1
                            sid = self.session_sequence
                            self.addr2sid_map[addr] = sid
                            self.sid2addr_map[sid] = addr
                        else:
                            sid = self.addr2sid_map[addr]
                        packet_data_with_sid = Server.pack_udp(sid, packet_data)
                        self.server_client_udp_data.sendto(packet_data_with_sid, client_addr_udp)
                    elif packet_type == "->user":
                        packet_data_with_sid, _ = origin_sock.recvfrom(4096)
                        session_id, packet_data = Server.unpack_udp(packet_data_with_sid)
                        if session_id != 0:
                            if session_id in self.sid2addr_map:
                                user_addr = self.sid2addr_map[session_id]
                                self.server_public_udp.sendto(packet_data, user_addr)
                        elif session_id == 0:
                            print(str(packet_data))

    def handle_public_tcp(self, control_channel: socket.socket):
        self.tcp_sel.register(self.server_public_tcp, selectors.EVENT_READ, data="NewUser")
        while True:
            events = self.tcp_sel.select()
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    data = key.data
                    if data == "NewUser":
                        conn_user, addr1 = self.server_public_tcp.accept()
                        print(f"Connected by user: {addr1}")
                        control_channel.send(b'new')
                        conn_client, addr2 = self.server_control.accept()
                        print(f"Connected by client: {addr2}")
                        self.tcp_sel.register(conn_user, selectors.EVENT_READ, data=conn_client)
                        self.tcp_sel.register(conn_client, selectors.EVENT_READ, data=conn_user)
                        self.sockets.append(conn_user)
                        self.sockets.append(conn_client)
                    elif isinstance(data, socket.socket):
                        sock_a = cast(socket.socket, key.fileobj)
                        sock_b = key.data
                        try:
                            recv_data = sock_a.recv(1024)
                            if not recv_data:
                                self.tcp_sel.unregister(sock_a)
                                self.tcp_sel.unregister(sock_b)
                                sock_a.close()
                                sock_b.close()
                            else:
                                sock_b.sendall(recv_data)
                        except (ConnectionResetError, OSError):
                            # 关 socket 前必须清理注册 必须 unregister 先于 close——close 后 FD 失效，再 unregister 会报错。
                            self.tcp_sel.unregister(sock_a)
                            self.tcp_sel.unregister(sock_b)
                            sock_a.close()
                            sock_b.close()


if __name__ == '__main__':
    server = Server()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            server.quit()
            break
