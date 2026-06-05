from dataclasses import dataclass, asdict
import json
import socket
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
    NEW_USER = "NEW_USER"
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

# 具体命令类
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
    public_port: int = None

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.REGISTER

@register_cmd
@dataclass
class RegisterCompleteCommand(BaseCommand):
    public_port:int
    
    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.REGISTER_COMPLETE

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
class HolePunchingCommand(BaseCommand):
    public_port: int

    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.HOLE_PUNCHING

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

@register_cmd
@dataclass()
class HolePunchingCompleteCommand(BaseCommand):
    @classmethod
    def get_cmd_type(cls) -> ResponseType:
        return ResponseType.HOLE_PUNCHING_COMPLETE


class Protocol:
    @staticmethod
    def pack(cmd: BaseCommand) -> bytes:
        payload = {
            "cmd_type": cmd.get_cmd_type().value,
            "data": asdict(cmd)
        }
        return json.dumps(payload).encode()
    
    @staticmethod
    def unpack(raw: bytes) -> tuple[ResponseType, BaseCommand]:
        raw_data = json.loads(raw.decode())
        cmd_type = ResponseType(raw_data["cmd_type"])
        cmd_class = COMMAND_REGISTRY[cmd_type]
        cmd_instance = cmd_class(**raw_data["data"])
        return cmd_type, cmd_instance