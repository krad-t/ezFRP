from dataclasses import dataclass, field
import socket
from protocol import ResponseType

@dataclass
class BaseService:
    ctl: socket.socket        # 控制通道
    channel_type: ResponseType
    public_port: int = None


@dataclass
class TCPService(BaseService):
    public_listen_sock: socket.socket = None
    curr_free_client_data_channel: socket.socket = None


@dataclass
class UDPService(BaseService):
    client_addr: tuple = None
    session_counter: int = 0
    addr2sid: dict = field(default_factory=dict)   # (user_ip, user_port) -> session_id
    sid2addr: dict = field(default_factory=dict)   # session_id -> (user_ip, user_port)