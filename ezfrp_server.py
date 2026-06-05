from dataclasses import dataclass
import socket
import struct
import selectors
from typing import cast
from enum import IntEnum

from protocol import *
from ezfrp_service import *


class Tag(IntEnum):
    DSP_ACCEPT = 1  # _dispatch_listen 有新 TCP连接
    UNK_RECV = 2   # 处理_dispatch_listen accept的未知sock
    CTL_RECV = 3  # 已有 Client 的控制通道可读
    TCP_ACCEPT = 4  # 某公网 TCP 端口有新外部用户
    TCP_DATA = 5  # TCP socket pair 可读
    UDP_PUBLIC = 6  # 公网 UDP 收到数据
    UDP_CLIENT = 7  # Client 的 UDP 回复到达

class Server:

    def __init__(self):
        self.config = Server.configure()
        self._sockets = []
        self._dispatch_listen_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # todo修改config的字段名，以符合语义
        self._dispatch_listen_tcp.bind(("0.0.0.0", self.config['control_port']))
        self._dispatch_listen_tcp.listen(1)
        self._sockets.append(self._dispatch_listen_tcp)

        self._client_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client_udp.bind(("0.0.0.0", self.config['udp_data_port']))
        self._sockets.append(self._client_udp)
        self._sel = selectors.DefaultSelector()

        # 记录不同的服务组，1个Client-n个User的一种协议通道 定义为一组"服务"，和对外暴露的端口一一映射
        self._services:dict[int, BaseService] = dict()  
        self._log("Server initialized")
        self.run()

    def run(self):
        self._sel.register(self._dispatch_listen_tcp, selectors.EVENT_READ, data=(Tag.DSP_ACCEPT,))
        while True:
            events = self._sel.select()
            for key, mask in events:
                sock = cast(socket.socket, key.fileobj)
                tag = key.data[0]
                args = key.data[1:]
                if mask & selectors.EVENT_READ:
                    if tag == Tag.DSP_ACCEPT:
                        self._client_dispatch_acc(sock, *args)
                    elif tag == Tag.UNK_RECV:
                        self._client_handle_unk(sock, *args)
                    elif tag == Tag.CTL_RECV:
                        self._handle_ctl_cmd(sock, *args)
                    elif tag == Tag.TCP_ACCEPT:
                        self._tcp_accept_user(sock, *args)
                    elif tag == Tag.TCP_DATA:
                        self._tcp_data_trans(sock, *args)
                    elif tag == Tag.UDP_PUBLIC:
                        self._udp_data_u2c(sock, *args)
                    elif tag == Tag.UDP_CLIENT:
                        self._udp_data_c2u(sock, *args)
                    else:
                        self._log(f"Unknown tag: {tag}")

    def _assign_service_ports(self, service:BaseService) -> tuple[int, int]:
        # 从合法的port pool中占用一个
        # todo:换成占用池子里的port
        # self.config["max_port"]
        # self.config["min_port"]
        # ...
        public_port, client_port = 7000, 7001

        if service.channel_type == ResponseType.UDP:
            pass
        elif service.channel_type == ResponseType.TCP:
            pass
        else:
            self._log(f"Unknown channel type when assign service ports: {service.channel_type}")

        service.user_port = public_port
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
    def unpack_udp(data: bytes, method="!I") -> tuple[int, bytes]:
        session_id = struct.unpack(method, data[:4])[0]
        packet_data = data[4:]
        return session_id, packet_data

    def _client_dispatch_acc(self, dispatch_listen: socket.socket):
        # _dispatch_listen 有新 Client 此时sock == _dispatch_listen
        unknown_channel, _ = dispatch_listen.accept()
        self._sel.register(unknown_channel, selectors.EVENT_READ, data=(Tag.UNK_RECV,))
        self._log(f"Connected to client: {unknown_channel}")

    def _client_handle_unk(self, unknown_channel: socket.socket):
        raw_data, addr = unknown_channel.recv(1024)
        # 解析data的指令
        response_type, cmd_instance = Protocol.unpack(raw_data)
        # 如果是CLIENT_LOGIN，这表示Client第一次连接Server
        if response_type == ResponseType.CLIENT_LOGIN:
            # 说明这是一个控制通道，注册为CTL_RECV
            # todo 不传unknown_channel引用？
            unknown_channel.send(Protocol.pack(cmd=LoginCompleteCommand(service_ctl=unknown_channel)))
            self._sel.register(unknown_channel, selectors.EVENT_READ, data=(Tag.CTL_RECV,))
        elif response_type == ResponseType.SERVICE_BIND:
            # todo说明这是一个绑定服务的请求, 似乎只有TCP会从此绑定
            cmd_instance = cast(RegisterCommand, cmd_instance)
            sid = cmd_instance.service_id
            service = self._services[sid]
            cast(TCPService, service).curr_free_client_data_channel = unknown_channel
            # 不需要注册，因为要在_tcp_accept_user里注册这个连接和公网用户的连接为TCP_DATA
            # self._sel.register(unknown_channel, selectors.EVENT_READ, data=(Tag.TCP_DATA,sid))
            
        

    def _handle_ctl_cmd(self, ctl_channel: socket.socket):
        # 已有 Client 的控制通道可读
        raw_data, addr = ctl_channel.recv(1024)
        response_type, cmd_instance = Protocol.unpack(raw_data)
        if response_type == ResponseType.REGISTER:
            # Client 发来注册数据通道的请求
            # todo RegisterCommand和RegisterCompleteCommand 要做更精细的设计，以防上一个注册还没完成，Client就又发来一个注册请求了，导致回复的RegisterCompleteCommand和Client的RegisterCommand不匹配了
            cmd_instance = cast(RegisterCommand, cmd_instance)
            
            if cmd_instance.get_cmd_type == ResponseType.UDP:
                new_service = UDPService(
                    ctl=ctl_channel,
                    channel_type=ResponseType.UDP,
                )
                public_port, _ = self._assign_service_ports(new_service)

                self._services[public_port] = new_service
                new_service.ctl.send(Protocol.pack(cmd=RegisterCompleteCommand(public_port=public_port)))
            elif cmd_instance.get_cmd_type == ResponseType.TCP:
                public_listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                public_listen_sock.bind(("0.0.0.0", public_port))
                public_listen_sock.listen(1)

                new_service = TCPService(
                    ctl=ctl_channel,
                    channel_type=ResponseType.TCP,
                    public_listen_sock=public_listen_sock
                )
                public_port, _ = self._assign_service_ports(new_service)
                self._services[public_port] = new_service

                self._sel.register(public_listen_sock, selectors.EVENT_READ, data=(Tag.TCP_ACCEPT, new_service))
                new_service.ctl.send(Protocol.pack(cmd=RegisterCompleteCommand(public_port=public_port)))
            else:
                self._log(f"Unsupported channel type in REGISTER cmd: {cmd_instance.channel_type}")
        elif response_type == ResponseType.HOLE_PUNCHING:
            # Client 发来打洞命令，说明服务已经创建好了，获取地址即可
            cmd_instance = cast(HolePunchingCommand, cmd_instance)
            client_udp_addr = (ctl_channel.getsockname()[0], addr[1])
            service = self._services[cmd_instance.public_port]
            service.client_addr = client_udp_addr
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.bind(("0.0.0.0", self.config['udp_data_port']))
            public_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            public_port = cmd_instance.public_port
            public_sock.bind(("0.0.0.0", public_port))
            self._sel.register(public_sock, selectors.EVENT_READ, data=(Tag.UDP_PUBLIC, client_sock, service))
            self._sel.register(client_sock, selectors.EVENT_READ, data=(Tag.UDP_CLIENT, public_sock, service))


    def _tcp_accept_user(self, sock_listen, service: TCPService):
        # 某公网 TCP 端口有新外部用户
        conn_user, addr_user = sock_listen.accept()
        conn_client = service.curr_free_client_data_channel
        # 先使用掉空闲的连接
        self._sel.register(conn_user, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_client))
        self._sel.register(conn_client, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_user))
        # 再通知Client空闲的连接被新User占用，Client需要新建连接
        service.curr_free_client_data_channel = None
        service.ctl.send(Protocol.pack(cmd=NewUserCommand(public_port=service.public_port)))
        self._log(f"TCP pair established: [U]{conn_user.getsockname()}<-->[C]{conn_client.getsockname()} for service on port {service.public_port}")


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

    def _udp_data_u2c(self, user_sock, client_sock: socket.socket, service: UDPService):
        # 公网 UDP 收到数据,此时才能创建session，并通过_client_udp发送数据
        recv_data, addr = user_sock.recvfrom(2048)
        # 查找session id
        if addr not in service.addr2sid:
            service.session_counter += 1
            sid = service.session_counter
            service.addr2sid[addr] = sid
            service.sid2addr[sid] = addr
        else:
            sid = service.addr2sid[addr]
        packed_data = self.pack_udp(sid, recv_data)
        client_sock.sendto(packed_data, service.client_addr)

    def _udp_data_c2u(self, client_sock, user_sock: socket.socket, service: UDPService):
        # Client 的 UDP 回复到达
        recv_data, addr = client_sock.recvfrom(2048)
        # 找session id
        sid, raw_data = self.unpack_udp(recv_data)
        user_sock.sendto(raw_data, service.sid2addr[sid])


if __name__ == '__main__':
    server = Server()
    while True:
        cli_cmd = input("q to quit:\n")
        if cli_cmd == 'q':
            server.quit()
            break
