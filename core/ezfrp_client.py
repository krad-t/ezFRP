import os
import struct
import selectors
from enum import IntEnum
from typing import cast, Any

from .ezfrp_service import *
from .protocol import *


class Tag(IntEnum):
    LOGIN_SUCCESS = 1  # _dispatch_listen 有新 TCP连接
    CTL_RECV = 2  # 控制通道收到消息
    TCP_RECV = 3  # TCP pair 可读
    UDP_SERVER = 4
    UDP_LOCAL = 5
    QUIT = 6


class Client:
    def __init__(self):
        self._config = self.configure()
        self._service_configs: list[dict[str, Any]] = self._config.get("services", [])
        self._sel = selectors.DefaultSelector()
        self._sockets = []
        self._ctl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._udp_data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_data_sock.bind(("0.0.0.0", 0))

        self._sig_r, self._sig_w = socket.socketpair()
        self._running = True

        self._services: dict[int, BaseService] = dict()

    def run(self):
        self._sel.register(self._sig_r, selectors.EVENT_READ, data=(Tag.QUIT,))
        self._sel.register(self._ctl, selectors.EVENT_READ, data=(Tag.CTL_RECV, FrameBuffer()))
        self._sel.register(self._udp_data_sock, selectors.EVENT_READ, data=(Tag.UDP_SERVER,))
        self._ctl.connect((self._config['server_ip'], self._config['tcp_endpoint']))
        self._log(f"Client connected to Server(1/2)")
        self._ctl.send(Protocol.pack(cmd=ClientLoginCommand()))

        while self._running:
            events = self._sel.select(timeout=15)
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    sock = cast(socket.socket, key.fileobj)
                    tag = key.data[0]
                    args = key.data[1:]
                    if tag == Tag.CTL_RECV:
                        self._handle_ctl(sock, *args)
                    elif tag == Tag.TCP_RECV:
                        self._tcp_data_trans(sock, *args)
                    elif tag == Tag.UDP_SERVER:
                        self._udp_data_s2l(sock)
                    elif tag == Tag.UDP_LOCAL:
                        self._udp_data_l2s(sock, *args)
                    elif tag == Tag.QUIT:
                        self._log("Shutting down...")
                        self._ctl.close()
                        self._running = False
                        break
            for s in self._services.values():
                if s.channel_type == ResponseType.UDP:
                    self._hole_punching(s.public_port)

    def signal_quit(self):
        self._sig_w.send(b"\x00")

    def _hole_punching(self, public_port: int):
        self._udp_data_sock.sendto(self.pack_udp(sid=0, public_port=public_port, data=b''),
                                   (self._config["server_ip"], self._config["udp_endpoint"]))

    def _handle_ctl(self, ctl, frame_buf: FrameBuffer):
        raw_data = ctl.recv(1024)
        if not raw_data:
            self._log("Server closed")
            self._sel.unregister(self._ctl)
            self._ctl.close()
            return
        try:
            frames = frame_buf.feed(raw_data)
        except ValueError:
            self._log("Bad protocol data from server, ignoring")
            return
        for response_type, cmd_instance in frames:
            if response_type == ResponseType.CLIENT_LOGIN_COMPLETE:
                self._log("Client login complete(2/2)")
                self._register_all_services(ctl)
            elif response_type == ResponseType.REGISTER_COMPLETE:
                cmd_instance = cast(RegisterCompleteCommand, cmd_instance)
                public_port = cmd_instance.public_port
                channel_type = cmd_instance.channel_type
                svc_cfg = self._service_configs[cmd_instance.service_id]
                self._log(f"{public_port}")
                if channel_type == ResponseType.TCP:
                    new_service = TCPServiceClient(
                        ctl=ctl,
                        channel_type=channel_type,
                        public_port=public_port,
                        local_host=svc_cfg["local_host"],
                        local_port=svc_cfg["local_port"],
                    )
                    self._services[public_port] = new_service

                elif channel_type == ResponseType.UDP:
                    new_service = UDPServiceClient(
                        ctl=ctl,
                        public_port=public_port,
                        channel_type=channel_type,
                        local_host=svc_cfg["local_host"],
                        local_port=svc_cfg["local_port"],
                    )
                    self._services[public_port] = new_service
                    ctl.send(Protocol.pack(cmd=ReadyHolePunchingCommand(public_port)))
                else:
                    self._log(f"Unknown channel type {channel_type}")

                self._log(
                    f"{channel_type} channel established, using {self._config['server_ip']}:{public_port} to connect")
            elif response_type == ResponseType.REGISTER_FAIL:
                msg = cast(RegisterFailCommand, cmd_instance).msg
                self._log(f"Client register failed, {msg}")
                # todo 后期加上指定次数的自动重试？
                continue
            elif response_type == ResponseType.NEW_USER:
                cmd_instance = cast(NewUserCommand, cmd_instance)
                service = cast(TCPServiceClient, self._services.get(cmd_instance.public_port))
                if service is None:
                    self._log(f"NEW_USER for unknown public port {cmd_instance.public_port}, ignored")
                    continue
                n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                n_socket.connect((self._config['server_ip'], self._config['tcp_endpoint']))
                n_socket.send(Protocol.pack(
                    cmd=ServiceBindCommand(public_port=cmd_instance.public_port, channel_type=ResponseType.TCP)))
                try:
                    local_app_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    local_app_sock.connect((service.local_host, service.local_port))
                except OSError:
                    self._log(f"Local app at {service.local_host}:{service.local_port} not reachable, "
                              f"dropping new user connection")
                    n_socket.close()
                    continue
                self._sel.register(n_socket, selectors.EVENT_READ, data=(Tag.TCP_RECV, local_app_sock))
                self._sel.register(local_app_sock, selectors.EVENT_READ, data=(Tag.TCP_RECV, n_socket))
            elif response_type == ResponseType.HOLE_PUNCHING:
                cmd_instance = cast(HolePunchingCommand, cmd_instance)
                self._log(f"HOLE_PUNCHING{cmd_instance.public_port}")
                self._hole_punching(public_port=cmd_instance.public_port)
            elif response_type == ResponseType.HOLE_PUNCHING_COMPLETE:
                cmd_instance = cast(HolePunchingCompleteCommand, cmd_instance)
                self._log(f"hole punch complete")
            else:
                self._log("Client receive unknown response")

    def _register_all_services(self, ctl):
        """登录完成后，按配置的 services 列表逐个向 Server 注册数据通道"""
        for service_id, svc in enumerate(self._service_configs):
            try:
                channel_type = ResponseType(svc["channel_type"])
            except ValueError:
                self._log(f"Invalid channel_type in services[{service_id}]: {svc.get('channel_type')}, skipped")
                continue
            self._log(f"Registering service[{service_id}] {channel_type} "
                      f"local {svc['local_host']}:{svc['local_port']} "
                      f"public_port={svc.get('public_port', 0) or 'auto'}")
            ctl.send(Protocol.pack(cmd=RegisterCommand(
                channel_type=channel_type,
                public_port=svc.get("public_port", 0),
                service_id=service_id,
            )))

    def _tcp_data_trans(self, sock_a: socket.socket, sock_b: socket.socket):
        # TCP socket pair 可读
        try:
            recv_data = sock_a.recv(65536)
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

    def _udp_data_s2l(self, udp_data_sock: socket.socket):
        recv_data, addr = udp_data_sock.recvfrom(2048)
        sid, public_port, data = self.unpack_udp(recv_data)
        service = cast(UDPServiceClient, self._services[public_port])
        fake_user_sock = None
        if sid not in service.sid2sock:
            service.session_counter += 1
            # 新建一个本地的sock，假装是"局域网内的sock"
            fake_user_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            fake_user_sock.bind(("0.0.0.0", 0))
            service.sid2sock[sid] = fake_user_sock
            service.sock2sid[fake_user_sock] = sid
            self._sel.register(fake_user_sock, selectors.EVENT_READ, data=(Tag.UDP_LOCAL, sid, public_port))
        else:
            fake_user_sock = service.sid2sock[sid]

        # 由假装的sock向本地应用发送数据
        fake_user_sock.sendto(data, (service.local_host, service.local_port))

    def _udp_data_l2s(self, fake_user_sock: socket.socket, sid: int, port: int):
        try:
            recv_data, addr = fake_user_sock.recvfrom(2048)
        except (ConnectionResetError, OSError):
            # 本地 UDP 应用不可达时（如 Windows 的 ICMP 端口不可达反馈），关闭该会话
            service = cast(UDPServiceClient, self._services.get(port))
            if service is not None:
                service.sid2sock.pop(sid, None)
                service.sock2sid.pop(fake_user_sock, None)
            try:
                self._sel.unregister(fake_user_sock)
            except (KeyError, ValueError):
                pass
            fake_user_sock.close()
            self._log(f"Local UDP app unreachable, session {sid} on public port {port} closed")
            return
        service = cast(UDPServiceClient, self._services[port])
        packed_data = self.pack_udp(sid, port, recv_data)
        self._udp_data_sock.sendto(packed_data, (self._config["server_ip"], self._config["udp_endpoint"]))

    def _log(self, msg, msg_before=None):
        if msg_before is not None:
            print(msg_before)
        from datetime import datetime
        print(f"[{type(self).__name__}//{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}")

    @staticmethod
    def configure():
        import json
        REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(REPO_ROOT, 'config', 'ezfrp_client.json')
        try:
            with open(path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            print('ezfrp_client.json not found, creating a new one using default configuration')
            config = {
                "server_ip": "127.0.0.1",
                "tcp_endpoint": 7000,
                "udp_endpoint": 7001,
                "services": [
                    {"local_host": "127.0.0.1", "local_port": 8080, "public_port": 0, "channel_type": "TCP"},
                    {"local_host": "127.0.0.1", "local_port": 5000, "public_port": 0, "channel_type": "UDP"}
                ]
            }
            with open(path, 'w') as f:
                f.write(json.dumps(config, indent=2))
        return config

    @staticmethod
    def pack_udp(sid: int, public_port: int, data: bytes, sid_fmt="!I", port_fmt="I") -> bytes:
        return struct.pack(sid_fmt + port_fmt, sid, public_port) + data

    @staticmethod
    def unpack_udp(data: bytes, sid_fmt="!I", port_fmt="I") -> tuple[int, int, bytes]:
        header_size = struct.calcsize(sid_fmt + port_fmt)
        session_id, public_port = struct.unpack(sid_fmt + port_fmt, data[:header_size])
        packet_data = data[header_size:]
        return session_id, public_port, packet_data

    def quit(self):
        self._ctl.close()
        import sys
        sys.exit(0)


if __name__ == '__main__':
    import threading

    client = Client()
    t = threading.Thread(target=client.run)
    t.start()

    while True:
        if input() == 'q':
            client.signal_quit()
            break
    t.join()
