import collections
from dataclasses import dataclass, asdict
import json
import socket
import struct
from typing import Type
from abc import ABC, abstractmethod
from enum import StrEnum


class ResponseType(StrEnum):
    TCP = "TCP"
    UDP = "UDP"
    CLIENT_LOGIN = "CLIENT_LOGIN"
    CLIENT_LOGIN_COMPLETE = "CLIENT_LOGIN_COMPLETE"
    SERVICE_BIND = "SERVICE_BIND"
    REGISTER = "REGISTER"
    REGISTER_COMPLETE = "REGISTER_COMPLETE"
    REGISTER_FAIL = "REGISTER_FAIL"
    NEW_USER = "NEW_USER"
    READY_HOLE_PUNCHING = "READY_HOLE_PUNCHING"
    HOLE_PUNCHING = "HOLE_PUNCHING"
    HOLE_PUNCHING_COMPLETE = "HOLE_PUNCHING_COMPLETE"


# 全局命令注册表
COMMAND_REGISTRY: dict[ResponseType, Type] = {}


@dataclass
class BaseCommand(ABC):
    """所有命令的基类"""

    @classmethod
    @abstractmethod
    def get_cmd_type(cls) -> ResponseType:
        """子类必须实现：返回对应的 ResponseType"""
        pass


def register_cmd(cls: Type[BaseCommand]) -> Type[BaseCommand]:
    """装饰器：自动将命令类注册到全局表"""
    cmd_type = cls.get_cmd_type()
    COMMAND_REGISTRY[cmd_type] = cls
    return cls


################################
# 具体命令类（带数据）
################################
@register_cmd
@dataclass
class NewUserCommand(BaseCommand):
    public_port: int = None

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.NEW_USER


@register_cmd
@dataclass
class RegisterCommand(BaseCommand):
    channel_type: ResponseType
    public_port: int = 0  # 0 = 由 Server 自动分配；非 0 = Client 声明需要的公网端口
    service_id: int = 0  # Client 侧服务编号，Server 原样回传，用于 REGISTER_COMPLETE 回映到配置项

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.REGISTER


@register_cmd
@dataclass
class RegisterCompleteCommand(BaseCommand):
    public_port: int
    channel_type: ResponseType
    service_id: int = 0

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.REGISTER_COMPLETE


@register_cmd
@dataclass
class RegisterFailCommand(BaseCommand):
    channel_type: ResponseType
    service_id: int = 0
    msg: str = ""

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.REGISTER_FAIL


@register_cmd
@dataclass
class ServiceBindCommand(BaseCommand):
    public_port: int
    channel_type: ResponseType

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.SERVICE_BIND


@register_cmd
@dataclass
class ReadyHolePunchingCommand(BaseCommand):
    public_port: int

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.READY_HOLE_PUNCHING


@register_cmd
@dataclass
class HolePunchingCommand(BaseCommand):
    public_port: int

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.HOLE_PUNCHING


@register_cmd
@dataclass()
class HolePunchingCompleteCommand(BaseCommand):
    public_port: int

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.HOLE_PUNCHING_COMPLETE


################################
# 没有数据的命令
################################
@register_cmd
@dataclass
class LoginCompleteCommand(BaseCommand):
    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.CLIENT_LOGIN_COMPLETE


@register_cmd
@dataclass
class ClientLoginCommand(BaseCommand):
    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.CLIENT_LOGIN


################################
#
################################

class Protocol:
    # 帧头：4 字节大端长度前缀 + JSON 载荷。一次 recv 可能包含多条/半条帧，
    # 接收侧用 FrameBuffer 缓冲拆帧。
    FRAME_HEADER = struct.Struct("!I")
    MAX_FRAME_SIZE = 1 << 20  # 1MB，防坏数据撑爆内存(4字节无符号整数最大支持载荷是4GB,太大了)

    @staticmethod
    def pack(cmd: BaseCommand) -> bytes:
        payload = {
            "cmd_type": cmd.get_cmd_type().value,
            "data": asdict(cmd)
        }
        body = json.dumps(payload).encode()
        return Protocol.FRAME_HEADER.pack(len(body)) + body

    @staticmethod
    def unpack(body: bytes) -> tuple[ResponseType, BaseCommand]:
        """解析一条完整 JSON 载荷（不含帧头）"""
        raw_data = json.loads(body.decode())
        try:
            cmd_type = ResponseType(raw_data["cmd_type"])
            cmd_class = COMMAND_REGISTRY[cmd_type]
            cmd_instance = cmd_class(**raw_data["data"])
            return cmd_type, cmd_instance
        except ValueError as e:
            # cmd_type不是内置的ResponseType 无法通过ResponseType(raw_data["cmd_type"])实例化某一种type
            raise ValueError(f"bad cmd_type: {raw_data['cmd_type']}") from e
        except TypeError as e:
            # rawdata传来的实际数据解析出的字段和当前的cmd_type不匹配
            raise TypeError(f"miss match data {raw_data['data']} to cmd {cmd_type}") from e


class FrameBuffer:
    """按 [4字节大端长度][JSON载荷] 拆帧的接收缓冲。

    每个会话（控制通道/未知通道）维护一个实例：
    feed() 塞入 recv 到的原始字节，返回本次解析出的全部完整命令；
    剩余不足一帧的字节留在缓冲里，等下次 recv 继续喂。
    """

    def __init__(self):
        self._buf = b""
        self.drop = collections.deque(maxlen=100)

    def feed(self, data: bytes) -> list[tuple[ResponseType, BaseCommand]]:
        self._buf += data
        frames = []
        while True:
            if len(self._buf) < Protocol.FRAME_HEADER.size:
                break
            (length,) = Protocol.FRAME_HEADER.unpack(self._buf[:Protocol.FRAME_HEADER.size])
            if length == 0 or length > Protocol.MAX_FRAME_SIZE:
                raise ValueError(f"bad frame length {length}")
            if len(self._buf) < Protocol.FRAME_HEADER.size + length:
                break  # 半条帧，等待后续数据
            body = self._buf[Protocol.FRAME_HEADER.size:Protocol.FRAME_HEADER.size + length]
            try:
                frames.append(Protocol.unpack(body))
            except (ValueError, TypeError) as e:
                self.drop.append((str(e), body[:64]))
            finally:
                # 跳过
                self._buf = self._buf[Protocol.FRAME_HEADER.size + length:]
        return frames
