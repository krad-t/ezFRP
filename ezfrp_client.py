import socket
import struct
import threading
import selectors
from typing import cast

from protocol import *


class Client:
    def __init__(self):
        self._config = self.configure()
        self._sockets = []
        self._tcp_sel = selectors.DefaultSelector()
        self._udp_sel = selectors.DefaultSelector()
        self._ctl_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._ctl_listen.connect((self._config['server_ip'], self._config['control_port']))
        self._sockets.append(self._ctl_listen)
        self.control_thread = threading.Thread(target=self.handle_control, daemon=True)
        self.control_thread.start()

        self._sid2sock = dict()  # 维护一个socket映射表，key是来自服务端UDP包的session_id，value是本地的的socket对象实例
        self._sock2sid = dict() # 维护一个反向映射表，key是本地的socket对象实例，value是来自session_id

    def _log(self, msg):
        from datetime import datetime
        print(f"[{type(self).__name__}//{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}")

    @staticmethod
    def configure():
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

    @staticmethod
    def pack_udp(sid: int, data: bytes, method="!I") -> bytes:
        return struct.pack(method, sid) + data

    @staticmethod
    def unpack_udp(data: bytes, method="!I") -> (int, bytes):
        session_id = struct.unpack(method, data[:4])[0]
        packet_data = data[4:]
        return session_id, packet_data

    def handle_control(self):
        self._log(f"Successfully connected to server {self._config['server_ip']}")
        while True:
            choice = input("Establishing TCP(1) or UDP(2) tunnel:")
            if choice == '1':
                self._ctl_listen.send(bytes('TCP', "utf-8"))
                # daemon继承
                # threading.Thread(target=self.handle_control_tcp).start()
                self.handle_control_tcp()
                break
            elif choice == '2':
                self._ctl_listen.send(bytes('UDP', "utf-8"))
                client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client_sock.bind(("0.0.0.0", 0)) # 绑定一个随机端口作为和Server转发UDP data的出入口
                
                # threading.Thread(target=self.handle_control_udp, args=(client_sock,)).start()
                self.handle_control_udp(client_sock)
                break

    def handle_control_udp(self, client_socket: socket.socket):
        self._log(f"client socket bound to {client_socket.getsockname()}")
        cmd = self._ctl_listen.recv(1024).decode("utf-8")
        # 等待接收Server打洞的指令，然后Client就可以发送一个UDP包过去，让Server知道被NAT映射后的端口
        self._log(f"client command received: {cmd}")
        if cmd == "UDP_HOLE_PUNCHING":
            data = Client.pack_udp(0,b'UDP_HOLE_PUNCHING')
            client_socket.sendto(data, (self._config['server_ip'], self._config['udp_data_port'])) # NAT 打洞

        def keepalive():
            import time
            while True:
                time.sleep(10)
                data = Client.pack_udp(0,b'UDP_KEEPALIVE')
                client_socket.sendto(data, (self._config['server_ip'], self._config['udp_data_port']))

        threading.Thread(target=keepalive, daemon=True).start()

        self._udp_sel.register(client_socket, selectors.EVENT_READ, data=(TAG_UDP_TO_LOCAL, 0))
        while True:
            events = self._udp_sel.select()
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    origin_sock = cast(socket.socket, key.fileobj)
                    packet_type = key.data[0]
                    if packet_type == TAG_UDP_TO_LOCAL:
                        packet_with_sid, _ = origin_sock.recvfrom(4096)
                        sid, packet_data = Client.unpack_udp(packet_with_sid)
                        if sid not in self._sid2sock:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            sock.bind((self._config['local_host'], 0))
                            self._sid2sock[sid] = sock
                            self._sock2sid[sock] = sid
                            self._udp_sel.register(sock, selectors.EVENT_READ, data=(TAG_UDP_TO_SERVER, sid))

                        sock = self._sid2sock[sid]
                        sock.sendto(packet_data, (self._config['local_host'], self._config['local_port']))
                    elif packet_type == TAG_UDP_TO_SERVER:
                        packet_data, _ = origin_sock.recvfrom(4096)
                        sid = key.data[1]
                        packet_with_sid = Client.pack_udp(sid, packet_data)
                        client_socket.sendto(packet_with_sid, (self._config['server_ip'], self._config['udp_data_port']))


    def handle_control_tcp(self):
        self._tcp_sel.register(self._ctl_listen, selectors.EVENT_READ, data=TAG_TCP_ACCEPT)
        while True:
            events = self._tcp_sel.select()
            for key, mask in events:
                data = key.data
                if data == TAG_TCP_ACCEPT:
                    sock = cast(socket.socket, key.fileobj)
                    cmd = sock.recv(1024).decode('utf-8')
                    if cmd == 'new':
                        self._log(f"new command {cmd}")
                        server_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        server_data.connect((self._config['server_ip'], self._config['control_port']))
                        self._sockets.append(server_data)
                        local_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        local_data.connect((self._config['local_host'], self._config['local_port']))
                        self._sockets.append(local_data)
                        self._tcp_sel.register(server_data, selectors.EVENT_READ, data=local_data)
                        self._tcp_sel.register(local_data, selectors.EVENT_READ, data=server_data)
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
                            sock_b.send(recv_data)
                    except (ConnectionResetError, OSError):
                        self._tcp_sel.unregister(sock_a)
                        self._tcp_sel.unregister(sock_b)
                        sock_a.close()
                        sock_b.close()

    def send_cmd(self, param):
        pass

    def quit(self):
        for s in self._sockets:
            s.close()
        for s in self._sock2sid:
            s.close()

if __name__ == '__main__':
    client = Client()
    client.control_thread.join()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            break

