import os
import struct
import selectors
import heapq
from typing import cast
from enum import IntEnum

from .protocol import *
from .ezfrp_service import *


class Tag(IntEnum):
    DSP_ACCEPT = 1  # _dispatch_listen 有新 TCP连接
    UNK_RECV = 2  # 处理_dispatch_listen accept的未知sock
    CTL_RECV = 3  # 已有 Client 的控制通道可读
    TCP_ACCEPT = 4  # 某公网 TCP 端口有新外部用户
    TCP_DATA = 5  # TCP socket pair 可读
    UDP_PUBLIC = 6  # 公网 UDP 收到数据
    UDP_CLIENT = 7  # Client 的 UDP 回复到达


class Server:
    class PortAllocator:
        """公网端口分配器：负责 min_port-max_port 段的预留与释放"""

        def __init__(self, min_port, max_port):
            self._data = []
            self._used = {}
            for port in range(min_port, max_port + 1):
                heapq.heappush(self._data, port)

        def get_a_free_port(self) -> int:
            try:
                port = heapq.heappop(self._data)
                self._used[port] = True
                return port
            except IndexError as e:
                raise IndexError("No available port") from e

        def reserve(self, port: int) -> bool:
            """尝试预留指定端口；成功返回 True，端口被占用或不在池内返回 False"""
            if self.is_used(port) or port not in self._data:
                return False
            self._data.remove(port)
            heapq.heapify(self._data)
            self._used[port] = True
            return True

        def free_a_port(self, port: int):
            if self.is_used(port):
                heapq.heappush(self._data, port)
                self._used.pop(port, None)
            else:
                return

        def is_used(self, port: int) -> bool:
            return port in self._used

    def __init__(self):
        self.config = Server.configure()
        self._portAllocator = None
        self._dispatch_listen_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._dispatch_listen_tcp.bind(("0.0.0.0", self.config['tcp_endpoint']))
        self._dispatch_listen_tcp.listen(10)

        self._client_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client_udp.bind(("0.0.0.0", self.config['udp_endpoint']))
        self._sel = selectors.DefaultSelector()

        # 记录不同的服务组，"连接1个Client和n个User的1种协议通道,也即特定的一种协议通道" 定义为一组"服务"，服务和对外暴露的端口一一映射
        self._services: dict[int, BaseService] = dict()
        self._log("Server initialized")

    def run(self):
        self._sel.register(self._dispatch_listen_tcp, selectors.EVENT_READ, data=(Tag.DSP_ACCEPT,))
        self._sel.register(self._client_udp, selectors.EVENT_READ, data=(Tag.UDP_CLIENT,))
        while True:
            events = self._sel.select()
            for key, mask in events:
                sock = cast(socket.socket, key.fileobj)
                tag = key.data[0]
                args = key.data[1:]
                if mask & selectors.EVENT_READ:
                    if tag == Tag.DSP_ACCEPT:
                        self._client_dispatch_acc(sock)
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
                        self._udp_data_c2u(sock)
                    else:
                        self._log(f"Unknown tag: {tag}")

    def _assign_service_ports(self, service: BaseService, requested_port: int = 0) -> int:
        if self._portAllocator is None:
            self._portAllocator = Server.PortAllocator(min_port=self.config["min_port"],
                                                       max_port=self.config["max_port"])

        if requested_port and self._portAllocator.reserve(requested_port):
            public_port = requested_port
            self._log(f"Granted requested public port {public_port} for {service.channel_type} service")
        else:
            if requested_port:
                self._log(f"Requested port {requested_port} unavailable, auto-assigning")
            try:
                public_port = self._portAllocator.get_a_free_port()
            except IndexError as e:
                raise e

        if service.channel_type == ResponseType.UDP:
            service.public_port = public_port
        elif service.channel_type == ResponseType.TCP:
            service = cast(TCPService, service)
            public_listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                public_listen_sock.bind(("0.0.0.0", public_port))
            except OSError as e:
                self._log(f"Bind public port {public_port} failed: {e}, releasing port")
                self._portAllocator.free_a_port(public_port)
                public_listen_sock.close()
                raise
            public_listen_sock.listen(1)
            service.public_listen_sock = public_listen_sock
            service.public_port = public_port
        else:
            self._log(f"Unknown channel type when assign service ports: {service.channel_type}")

        return public_port

    def _log(self, msg):
        from datetime import datetime
        print(f"[{type(self).__name__}//{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}")

    @staticmethod
    def configure():
        import json
        REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(REPO_ROOT, 'config', 'ezfrp_server.json')
        try:
            with open(path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            print('ezfrp_server.json not found, creating a new one using default configuration')
            config = {
                "tcp_endpoint": 7000,
                "udp_endpoint": 7001,
                "max_port": 9999,
                "min_port": 9900
            }

            with open(path, 'w') as f:
                f.write(json.dumps(config))
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

    def _client_dispatch_acc(self, dispatch_listen: socket.socket):
        # _dispatch_listen 有新 Client 此时sock == _dispatch_listen
        unknown_channel, _ = dispatch_listen.accept()
        self._sel.register(unknown_channel, selectors.EVENT_READ, data=(Tag.UNK_RECV, FrameBuffer()))
        self._log(f"Connected to client: {unknown_channel.getpeername()}")

    def _client_handle_unk(self, unknown_channel: socket.socket, frame_buf: FrameBuffer):
        raw_data = unknown_channel.recv(1024)
        if not raw_data:
            self._log(f"Client disconnected: {unknown_channel.getpeername()}")
            self._sel.unregister(unknown_channel)
            unknown_channel.close()
            return

        # 解析data的指令（一次 recv 可能包含多条帧）
        try:
            frames = frame_buf.feed(raw_data)
        except ValueError:
            self._log(f"Bad protocol data from {unknown_channel.getpeername()}, dropping")
            self._log(f"{raw_data}")
            self._sel.unregister(unknown_channel)
            unknown_channel.close()
            return
        for response_type, cmd_instance in frames:
            # 如果是CLIENT_LOGIN，这表示Client第一次连接Server
            if response_type == ResponseType.CLIENT_LOGIN:
                # 说明这是一个控制通道，注册为CTL_RECV
                unknown_channel.send(Protocol.pack(cmd=LoginCompleteCommand()))
                self._sel.modify(unknown_channel, selectors.EVENT_READ, data=(Tag.CTL_RECV, frame_buf))
            elif response_type == ResponseType.SERVICE_BIND:
                # 说明这是一个绑定服务的请求, 只有TCP会从此绑定
                cmd_instance = cast(ServiceBindCommand, cmd_instance)
                public_port = cmd_instance.public_port
                service = cast(TCPService, self._services[public_port])
                if service.pending_users:
                    conn_user = service.pending_users.pop(0)
                    self._sel.register(conn_user, selectors.EVENT_READ, data=(Tag.TCP_DATA, unknown_channel))
                    self._sel.modify(unknown_channel, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_user))
                else:
                    self._sel.unregister(unknown_channel)
                    service.free_data_conns.append(unknown_channel)
            else:
                self._log(f"Unexpected command on unknown channel: {response_type}")

    def _handle_ctl_cmd(self, ctl_channel: socket.socket, frame_buf: FrameBuffer):
        # 已有 Client 的控制通道可读
        raw_data = ctl_channel.recv(1024)
        if not raw_data:
            self._cleanup_client(ctl_channel)
            return
        try:
            frames = frame_buf.feed(raw_data)
        except ValueError:
            self._log(f"Bad protocol data on ctl from {ctl_channel.getpeername()}, dropping connection")
            self._cleanup_client(ctl_channel)
            return
        for response_type, cmd_instance in frames:
            if response_type == ResponseType.REGISTER:
                # Client 发来注册数据通道的请求
                cmd_instance = cast(RegisterCommand, cmd_instance)
                try:
                    if cmd_instance.channel_type == ResponseType.UDP:
                        new_service = UDPService(
                            ctl=ctl_channel,
                            channel_type=ResponseType.UDP,
                        )
                        # 处理Server端口占满的情况,应该直接失败？毕竟此时意味着服务端端口已满？或者增加队列？但是那样又更复杂了——还要考虑队列满了情况？
                        # 不如就直接失败，返回给client一个消息表面端口已满
                        try:
                            public_port = self._assign_service_ports(new_service,
                                                                     requested_port=cmd_instance.public_port)
                            self._services[public_port] = new_service
                            new_service.ctl.send(Protocol.pack(
                                cmd=RegisterCompleteCommand(public_port=public_port,
                                                            channel_type=new_service.channel_type,
                                                            service_id=cmd_instance.service_id)))
                        except IndexError as e:
                            self._log(f"Failed to register {cmd_instance.service_id}: {e}, because no available port")
                            new_service.ctl.send(Protocol.pack(
                                cmd=RegisterFailCommand(channel_type=new_service.channel_type,
                                                        service_id=cmd_instance.service_id,
                                                        msg="Server busy, no available port")
                            ))

                    elif cmd_instance.channel_type == ResponseType.TCP:
                        new_service = TCPService(
                            ctl=ctl_channel,
                            channel_type=ResponseType.TCP
                        )
                        # 处理Server端口占满的情况，同上
                        try:

                            public_port = self._assign_service_ports(new_service,
                                                                     requested_port=cmd_instance.public_port)
                            self._services[public_port] = new_service

                            self._sel.register(new_service.public_listen_sock, selectors.EVENT_READ,
                                               data=(Tag.TCP_ACCEPT, new_service))
                            new_service.ctl.send(Protocol.pack(
                                cmd=RegisterCompleteCommand(public_port=public_port,
                                                            channel_type=new_service.channel_type,
                                                            service_id=cmd_instance.service_id)))
                        except IndexError as e:
                            self._log(f"Failed to register {cmd_instance.service_id}: {e}, because no available port")
                            new_service.ctl.send(Protocol.pack(
                                cmd=RegisterFailCommand(channel_type=new_service.channel_type,
                                                        service_id=cmd_instance.service_id,
                                                        msg="Server busy, no available port")
                            ))
                    else:
                        self._log(f"Unsupported channel type in REGISTER cmd: {cmd_instance.channel_type}")
                except OSError:
                    self._log(f"Failed to assign port for REGISTER from {ctl_channel.getpeername()}, skipped")
            elif response_type == ResponseType.READY_HOLE_PUNCHING:
                # Client 发来准备打洞命令，Server应该开启监听
                cmd_instance = cast(ReadyHolePunchingCommand, cmd_instance)
                service = cast(UDPService, self._services[cmd_instance.public_port])
                public_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                public_port = cmd_instance.public_port
                try:
                    public_sock.bind(("0.0.0.0", public_port))
                except OSError as e:
                    self._log(f"Bind public UDP port {public_port} failed: {e}, dropping service")
                    self._portAllocator.free_a_port(public_port)
                    del self._services[public_port]
                    public_sock.close()
                    continue
                service.public_sock = public_sock
                ctl_channel.send(Protocol.pack(cmd=HolePunchingCommand(public_port)))
                self._sel.register(public_sock, selectors.EVENT_READ, data=(Tag.UDP_PUBLIC, self._client_udp, service))
            else:
                self._log(f'Unknown response type {response_type}')

    def _cleanup_client(self, ctl_channel: socket.socket):
        """Client 控制通道断开或协议错误：清理其名下所有服务、释放端口与连接"""
        self._log(f"Client disconnected: {ctl_channel.getpeername()}")
        try:
            self._sel.unregister(ctl_channel)
        except (KeyError, ValueError):
            pass
        for s in list(self._services.values()):
            public_port = s.public_port
            if ctl_channel == s.ctl:
                if s.channel_type == ResponseType.TCP:
                    s = cast(TCPService, s)
                    if self._portAllocator is not None:
                        self._portAllocator.free_a_port(public_port)
                    self._sel.unregister(s.public_listen_sock)
                    s.public_listen_sock.close()
                    for conn_d in s.free_data_conns:
                        try:
                            self._sel.unregister(conn_d)
                        except (KeyError, ValueError):
                            pass
                        conn_d.close()
                    for conn_u in s.pending_users:
                        try:
                            self._sel.unregister(conn_u)
                        except (KeyError, ValueError):
                            pass
                        conn_u.close()
                    del self._services[public_port]
                elif s.channel_type == ResponseType.UDP:
                    s = cast(UDPService, s)
                    # UDP 服务：释放端口 + 关闭公网监听 sock（若有）
                    if self._portAllocator is not None:
                        self._portAllocator.free_a_port(public_port)
                    if s.public_sock is not None:
                        try:
                            self._sel.unregister(s.public_sock)
                        except (KeyError, ValueError):
                            pass
                        s.public_sock.close()
                    del self._services[public_port]
        ctl_channel.close()

    def _tcp_accept_user(self, sock_listen, service: TCPService):
        # 某公网 TCP 端口有新外部用户
        conn_user, addr_user = sock_listen.accept()
        if service.free_data_conns:
            conn_client = service.free_data_conns.pop(0)
            self._sel.register(conn_user, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_client))
            self._sel.register(conn_client, selectors.EVENT_READ, data=(Tag.TCP_DATA, conn_user))
            self._log(
                f"TCP pair established: [U]{conn_user.getsockname()}<-->[C]{conn_client.getsockname()} for service on port {service.public_port}")
        else:
            service.pending_users.append(conn_user)
            self._log(f"waiting for client to supply TCP data")
        # 再通知Client空闲的连接被新User占用，Client需要补充新建连接
        service.ctl.send(Protocol.pack(cmd=NewUserCommand(public_port=service.public_port)))

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

    def _udp_data_u2c(self, user_sock, client_sock: socket.socket, service: UDPService):
        # 公网 UDP 收到数据,此时才能创建session，并通过_client_udp发送数据
        recv_data, addr = user_sock.recvfrom(2048)
        # 查找session id
        if addr not in service.addr2sid:
            service.session_counter += 1
            sid = service.session_counter
            service.addr2sid[addr] = sid
            service.sid2addr[sid] = addr
            service.sid2usock[sid] = user_sock
        else:
            sid = service.addr2sid[addr]
        packed_data = self.pack_udp(sid, service.public_port, recv_data)
        if service.client_addr is None:
            return
        client_sock.sendto(packed_data, service.client_addr)

    def _udp_data_c2u(self, client_sock):
        # Client 的 UDP 回复到达
        # 可能是打洞的包
        recv_data, addr = client_sock.recvfrom(2048)
        # 找session id
        sid, public_port, raw_data = self.unpack_udp(recv_data)
        service = self._services.get(public_port)
        if service is None:
            # 服务可能刚被断连清理删掉，在途的保活/数据包直接丢弃
            self._log(f"UDP packet for unknown/closed service port {public_port}, dropping")
            return
        service = cast(UDPService, service)

        if sid == 0:
            # 打洞的包，只更新client_addr
            if service.client_addr is None:
                service.client_addr = (service.ctl.getpeername()[0], addr[1])
                service.ctl.send(Protocol.pack(cmd=HolePunchingCompleteCommand(public_port=public_port)))
            return
        else:
            user_sock = service.sid2usock[sid]
            user_sock.sendto(raw_data, service.sid2addr[sid])


if __name__ == '__main__':
    import threading

    s = Server()
    threading.Thread(target=s.run, daemon=True).start()
    while True:
        cli = input()
        if cli == 'q':
            print("quit")
            break
