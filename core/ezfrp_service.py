from dataclasses import dataclass, field
import socket
from .protocol import ResponseType

@dataclass
class BaseService:
    ctl: socket.socket        # 控制通道
    channel_type: ResponseType
    public_port: int = None


@dataclass
class TCPService(BaseService):
    public_listen_sock: socket.socket = None
    free_data_conns: list = field(default_factory=list)
    pending_users: list = field(default_factory=list)


@dataclass
class UDPService(BaseService):
    client_addr: tuple = None
    public_sock: socket.socket = None      # 公网 UDP 监听 sock（READY_HOLE_PUNCHING 时创建）
    session_counter: int = 0
    addr2sid: dict = field(default_factory=dict)   # (user_ip, user_port) -> session_id
    sid2addr: dict = field(default_factory=dict)   # session_id -> (user_ip, user_port)
    sid2usock: dict = field(default_factory=dict) # session_id -> user sock

#####################CLIENT SERVICE########################

@dataclass
class TCPServiceClient(BaseService):
    local_host: str = None
    local_port: int = None

@dataclass
class UDPServiceClient(BaseService):
    local_host: str = None
    local_port: int = None
    session_counter: int = 0
    sock2sid: dict = field(default_factory=dict)   # (user_ip, user_port) -> session_id
    sid2sock: dict = field(default_factory=dict)   # session_id -> (user_ip, user_port)
