from dataclasses import dataclass
import socket
import struct
import selectors
from typing import cast

from protocol import *


class Server:
    @dataclass
    class Service:
        session_id: int
        ctl: socket.socket
        client_port: int
        user_port: int
        channel_type: ResponseType
        addr2sid: dict
        sid2addr: dict
        client_addr: tuple

    def __init__(self):
        self.config = Server.configure()
        self._sockets = []
        self._ctl_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._ctl_listen.bind(("0.0.0.0", self.config['control_port']))
        self._ctl_listen.listen(1)
        self._sockets.append(self._ctl_listen)

        self._sel = selectors.DefaultSelector()

        self._services = dict()  # 记录不同的服务组，1个Client-n个User定义为一组"服务"

        self._log("Server initialized")
        self.run()

    def run(self):
        self._sel.register(self._ctl_listen, selectors.EVENT_READ, data=(Tag.CTL_ACCEPT,))
        while True:
            events = self._sel.select()
            for key, mask in events:
                sock = cast(socket.socket, key.fileobj)
                data = key.data
                if mask & selectors.EVENT_READ:
                    if data[0] == Tag.CTL_ACCEPT:
                        self._accept_client(sock)
                    elif data[0] == Tag.CTL_RECV:
                        self._handle_ctl_cmd(sock)
                    elif data[0] == Tag.TCP_ACCEPT:
                        self._tcp_accept_user(sock, data[1])
                    elif data[0] == Tag.TCP_DATA:
                        self._tcp_data_trans(sock, data[1])
                    elif data[0] == Tag.UDP_PUBLIC:
                        self._udp_data_u2c(sock, data[1])
                    elif data[0] == Tag.UDP_CLIENT:
                        self._udp_data_c2u(sock, data[1])
                    else:
                        self._log(f"Unknown tag: {data}")

    def _register_services(self, ctl_channel: socket.socket, channel_type):
        # 从合法的port pool中占用一个
        # todo:换成占用池子里的port
        # self.config["max_port"]
        # self.config["min_port"]
        # ...
        public_port, client_port = 7000, 7001

        if channel_type == ResponseType.UDP:
            udp_service = Server.Service(
                session_id=0,
                ctl=ctl_channel,
                client_port=client_port,
                user_port=public_port,
                channel_type=channel_type,
                addr2sid={},
                sid2addr={},
                client_addr=(),
            )
            self._services[ctl_channel] = udp_service
        elif channel_type == ResponseType.TCP:
            self._services[ctl_channel] = Server.Service(
                session_id=0,
                ctl=ctl_channel,
                client_port=client_port,
                user_port=public_port,
                channel_type=channel_type,
                addr2sid={},
                sid2addr={},
                client_addr=(),
            )
        else:
            self._log(f"Unknown channel type when gene service: {channel_type}")
        return public_port, client_port

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
            # todo修改默认的填充格式
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

    def _accept_client(self, sock: socket.socket):
        # _ctl_listen 有新 Client 此时sock == _ctl_listen
        ctl_channel, _ = sock.accept()
        self._sel.register(ctl_channel, selectors.EVENT_READ, data=(Tag.CTL_RECV,))
        self._log(f"Connected to client: {ctl_channel}")

    def _handle_ctl_cmd(self, ctl_channel: socket.socket):
        # 已有 Client 的控制通道可读
        cmd = Protocol.parse_cmd_from_client(ctl_channel.recv(1024))
        if cmd["response"] == ResponseType.REGISTER:
            if cmd["channel_type"] == ResponseType.TCP:
                public_port, _ = self._register_services(ctl_channel, ResponseType.TCP)
                tcp_public_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tcp_public_sock.bind(("0.0.0.0", public_port))
                self._sel.register(tcp_public_sock, selectors.EVENT_READ, data=(Tag.TCP_ACCEPT, ctl_channel))
                ctl_channel.send(Protocol.pack_cmd(response=ResponseType.OK, data=str(public_port)))
            elif cmd["channel_type"] == ResponseType.UDP:
                udp_public_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                public_port, client_port = self._register_services(ctl_channel, ResponseType.UDP)
                udp_public_sock.bind(("0.0.0.0", public_port))
                udp_client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                udp_client_sock.bind(("0.0.0.0", client_port))
                self._sel.register(udp_public_sock, selectors.EVENT_READ,
                                   data=(Tag.UDP_PUBLIC, ctl_channel, udp_client_sock))
                self._sel.register(udp_client_sock, selectors.EVENT_READ,
                                   data=(Tag.UDP_CLIENT, ctl_channel, udp_public_sock))
                ctl_channel.send(Protocol.pack_cmd(response=ResponseType.HOLE_PUNCHING, data=""))
            else:
                self._log(f"Unknown channel type: {cmd['channel_type']}")
                return
        elif cmd["response"] == ResponseType.HOLE_PUNCHING:
            client_port = cmd["data"]
            client_addr = (ctl_channel.getsockname()[0], client_port)
            service = self._services[ctl_channel]
            service.client_addr = client_addr
            ctl_channel.send(Protocol.pack_cmd(response=ResponseType.OK, data=str(service.user_port)))
        else:
            self._log(f"Unknown command: {cmd['request']}:{cmd['channel_type']}")

    def _tcp_accept_user(self, public_sock, ctl_channel: socket.socket):
        # 某公网 TCP 端口有新外部用户
        conn_user, addr_user = public_sock.accept()
        ctl_channel.send(Protocol.pack_cmd(response=ResponseType.NEW_USER, data=""))
        conn_client, addr_client = ctl_channel.accept()
        self._sel.register(conn_user, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_client))
        self._sel.register(conn_client, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_user))
        self._log(f"TCP pair registered: [U]{addr_user}<-->[C]{addr_client}")

    def _tcp_data_trans(self, sock_a, sock_b):
        # TCP socket pair 可读
        try:
            recv_data = sock_a.recv(1024)
            if not recv_data:
                self._sel.unregister(sock_a)
                self._sel.unregister(sock_b)
                sock_a.close()
                sock_b.close()
            else:
                sock_b.sendall(recv_data)
        except (ConnectionResetError, OSError):
            self._sel.unregister(sock_a)
            self._sel.unregister(sock_b)
            sock_a.close()
            sock_b.close()

    def _udp_data_u2c(self, user_sock, ctl, client_sock: socket.socket):
        # 公网 UDP 收到数据
        recv_data, addr = user_sock.recvfrom(2048)
        # 先查找是属于哪个服务
        service = self._services[ctl]
        # 此时再查找session id
        if addr not in service.addr2sid:
            service.session_id += 1
            service.addr2sid[addr] = service.session_id
            sid = service.session_id
        else:
            sid = service.addr2sid[addr]
        packed_data = self.pack_udp(sid, recv_data)
        client_sock.sendto(packed_data, service.client_addr)

    def _udp_data_c2u(self, client_sock, ctl, user_sock: socket.socket):
        # Client 的 UDP 回复到达
        recv_data, addr = client_sock.recvfrom(2048)
        # 查找服务
        service = self._services[ctl]
        # 再找session id
        sid, raw_data = self.unpack_udp(recv_data)
        user_sock.sendto(raw_data, service.sid2addr[sid])


if __name__ == '__main__':
    server = Server()
    while True:
        cli_cmd = input("q to quit:\n")
        if cli_cmd == 'q':
            server.quit()
            break
