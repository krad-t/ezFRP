import threading
import socket
import struct
import selectors
from typing import cast

from protocol import *


class Server:
    def __init__(self):
        config = Server.configure()
        self._tcp_sel = selectors.DefaultSelector()
        self._udp_sel = selectors.DefaultSelector()
        self._sockets = []
        self._ctl_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._ctl_listen.bind(("0.0.0.0", config['control_port']))
        self._ctl_listen.listen(1)
        self._sockets.append(self._ctl_listen)
        self._udp_client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_client.bind(("0.0.0.0", config['udp_data_port']))
        self._sockets.append(self._udp_client)
        threading.Thread(target=self.handle_control, daemon=True).start()

        self._tcp_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp_listen.bind(("0.0.0.0", config['public_tcp_port']))
        self._tcp_listen.listen(1)
        self._sockets.append(self._tcp_listen)

        self._udp_public = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_public.bind(("0.0.0.0", config['public_udp_port']))
        self._sockets.append(self._udp_public)

        self._addr2sid = dict()  # 维护一个反向映射表，key是用户地址，value是session_id
        self._sid2addr = dict()  # 维护一个会话映射表，key是session_id，value是用户地址
        self._next_sid = 0  # 维护一个全局会话序列号，每当有新的用户连接进来时，就自增1，生成一个新的session_id

        self._log("Server initialized")

    def quit(self):
        for s in self._sockets:
            s.close()

    def _log(self, msg):
        from datetime import datetime
        print(f"[{type(self).__name__}//{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}")

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
        while True:
            control_channel, addr = self._ctl_listen.accept()
            self._sockets.append(control_channel)
            self._log(f"Connected to client[control]:{addr}")
            while True:
                try:
                    client_cmd = control_channel.recv(1024).decode('utf-8')
                    # 处理控制消息
                    self._log(f"CMD [{client_cmd}] from {addr}")
                    # daemon继承
                    if client_cmd == 'TCP':
                        # threading.Thread(target=self.handle_public_tcp, args=(control_channel,)).start()
                        self.handle_public_tcp(control_channel)
                    elif client_cmd == 'UDP':
                        # threading.Thread(target=self.handle_public_udp, args=(control_channel,)).start()
                        self.handle_public_udp(control_channel)
                except (ConnectionResetError, OSError):
                    self._log("Client disconnected, waiting for reconnect...")
                    break

    def handle_public_udp(self, control_channel: socket.socket):
        # 若client 处于 NAT 设备之后，则这个端口就是错误的，需要靠UDP打洞来获取正确的地址
        control_channel.send(bytes("UDP_HOLE_PUNCHING", "utf-8"))  # 发送打洞指令给client，让client发送一个UDP包过来
        _, client_port_udp_nat = self._udp_client.recvfrom(4096)  # 从client发过来的UDP包中获取打洞后的端口
        client_port_udp_nat = client_port_udp_nat[1]
        client_addr_udp = (control_channel.getpeername()[0], int(client_port_udp_nat))
        self._log(f"Client UDP addr is {control_channel.getpeername()[0]}:{client_port_udp_nat}")

        self._udp_sel.register(self._udp_public, selectors.EVENT_READ, data=TAG_UDP_TO_CLIENT)
        self._udp_sel.register(self._udp_client, selectors.EVENT_READ, data=TAG_UDP_TO_USER)

        while True:
            events = self._udp_sel.select()
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    origin_sock = cast(socket.socket, key.fileobj)
                    packet_type = key.data
                    if packet_type == TAG_UDP_TO_CLIENT:
                        packet_data, addr = origin_sock.recvfrom(4096)
                        if addr not in self._addr2sid:
                            self._next_sid += 1
                            sid = self._next_sid
                            self._addr2sid[addr] = sid
                            self._sid2addr[sid] = addr
                        else:
                            sid = self._addr2sid[addr]
                        packet_data_with_sid = Server.pack_udp(sid, packet_data)
                        self._udp_client.sendto(packet_data_with_sid, client_addr_udp)
                    elif packet_type == TAG_UDP_TO_USER:
                        packet_data_with_sid, _ = origin_sock.recvfrom(4096)
                        session_id, packet_data = Server.unpack_udp(packet_data_with_sid)
                        if session_id != 0:
                            if session_id in self._sid2addr:
                                user_addr = self._sid2addr[session_id]
                                self._udp_public.sendto(packet_data, user_addr)
                        elif session_id == 0:
                            # print(str(packet_data))
                            pass

    def handle_public_tcp(self, control_channel: socket.socket):
        self._tcp_sel.register(self._tcp_listen, selectors.EVENT_READ, data=TAG_TCP_ACCEPT)
        while True:
            events = self._tcp_sel.select()
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    data = key.data
                    if data == TAG_TCP_ACCEPT:
                        conn_user, addr1 = self._tcp_listen.accept()
                        self._log(f"Connected by user: {addr1}")
                        control_channel.send(b'new')
                        conn_client, addr2 = self._ctl_listen.accept()
                        self._log(f"Connected to client: {addr2}")
                        self._tcp_sel.register(conn_user, selectors.EVENT_READ, data=conn_client)
                        self._tcp_sel.register(conn_client, selectors.EVENT_READ, data=conn_user)
                        self._sockets.append(conn_user)
                        self._sockets.append(conn_client)
                    elif isinstance(data, socket.socket):
                        sock_a = cast(socket.socket, key.fileobj)
                        sock_b = key.data
                        try:
                            recv_data = sock_a.recv(1024)
                            if not recv_data:
                                self._tcp_sel.unregister(sock_a)
                                self._tcp_sel.unregister(sock_b)
                                sock_a.close()
                                sock_b.close()
                            else:
                                sock_b.sendall(recv_data)
                        except (ConnectionResetError, OSError):
                            # 关 socket 前必须清理注册 必须 unregister 先于 close——close 后 FD 失效，再 unregister 会报错。
                            self._tcp_sel.unregister(sock_a)
                            self._tcp_sel.unregister(sock_b)
                            sock_a.close()
                            sock_b.close()


if __name__ == '__main__':
    server = Server()
    while True:
        cli_cmd = input("q to quit:\n")
        if cli_cmd == 'q':
            server.quit()
            break
