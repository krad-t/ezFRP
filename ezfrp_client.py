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
        self.tcp_sel = selectors.DefaultSelector()
        self.udp_sel = selectors.DefaultSelector()
        self.server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_control.connect((self.config['server_ip'], self.config['control_port']))
        self.sockets.append(self.server_control)
        self.control_thread = threading.Thread(target=self.handle_control, daemon=True)
        self.control_thread.start()

        self.sid2sock_map = dict()  # 维护一个socket映射表，key是来自服务端UDP包的session_id，value是本地的的socket对象实例
        self.sock2sid_map = dict() # 维护一个反向映射表，key是本地的socket对象实例，value是来自session_id

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
        print("Successfully connected to server", self.config['server_ip'])
        while True:
            choice = input("Establishing TCP(1) or UDP(2) tunnel:")
            if choice == '1':
                self.server_control.send(bytes('TCP',"utf-8"))
                # daemon继承
                # threading.Thread(target=self.handle_control_tcp).start()
                self.handle_control_tcp()
                break
            elif choice == '2':
                self.server_control.send(bytes('UDP',"utf-8"))
                client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                client_sock.bind(("0.0.0.0", 0)) # 绑定一个随机端口作为和Server转发UDP data的出入口
                
                # threading.Thread(target=self.handle_control_udp, args=(client_sock,)).start()
                self.handle_control_udp(client_sock)
                break

    def handle_control_udp(self, client_socket: socket.socket):
        print(f"client socket bound to {client_socket.getsockname()}")
        cmd = self.server_control.recv(1024).decode("utf-8")
        # 等待接收Server打洞的指令，然后Client就可以发送一个UDP包过去，让Server知道被NAT映射后的端口
        print(f"client command received: {cmd}")
        if cmd == "UDP_HOLE_PUNCHING":
            data = Client.pack_udp(0,b'UDP_HOLE_PUNCHING')
            client_socket.sendto(data, (self.config['server_ip'], self.config['udp_data_port'])) # NAT 打洞

        def keepalive():
            import time
            while True:
                time.sleep(10)
                data = Client.pack_udp(0,b'UDP_KEEPALIVE')
                client_socket.sendto(data, (self.config['server_ip'], self.config['udp_data_port']))

        threading.Thread(target=keepalive, daemon=True).start()

        self.udp_sel.register(client_socket, selectors.EVENT_READ, data=("->local",0))
        while True:
            events = self.udp_sel.select()
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    origin_sock = cast(socket.socket, key.fileobj)
                    packet_type = key.data[0]
                    if packet_type == "->local":
                        packet_with_sid, _ = origin_sock.recvfrom(4096)
                        sid, packet_data = Client.unpack_udp(packet_with_sid)
                        if sid not in self.sid2sock_map:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            sock.bind((self.config['local_host'], 0))
                            self.sid2sock_map[sid] = sock
                            self.sock2sid_map[sock] = sid
                            self.udp_sel.register(sock, selectors.EVENT_READ, data=("->server",sid))

                        sock = self.sid2sock_map[sid]
                        sock.sendto(packet_data,(self.config['local_host'], self.config['local_port']))
                    elif packet_type == "->server":
                        packet_data, _ = origin_sock.recvfrom(4096)
                        sid = key.data[1]
                        packet_with_sid = Client.pack_udp(sid, packet_data)
                        client_socket.sendto(packet_with_sid, (self.config['server_ip'], self.config['udp_data_port']))


    def handle_control_tcp(self):
        self.tcp_sel.register(self.server_control, selectors.EVENT_READ, data="NewUser")
        while True:
            events = self.tcp_sel.select()
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
                        self.tcp_sel.register(server_data, selectors.EVENT_READ, data=local_data)
                        self.tcp_sel.register(local_data, selectors.EVENT_READ, data=server_data)
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
                            sock_b.send(recv_data)
                    except (ConnectionResetError, OSError):
                        self.tcp_sel.unregister(sock_a)
                        self.tcp_sel.unregister(sock_b)
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

