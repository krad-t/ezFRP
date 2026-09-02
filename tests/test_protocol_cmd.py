from typing import cast

import pytest

from core.protocol import *


@dataclass()
class CmdData:
    public_port: int = 9900
    channel_type: ResponseType = ResponseType.TCP
    service_id: int = 0

testCMD_without_data: dict[ResponseType, Type] = {
    ResponseType.CLIENT_LOGIN_COMPLETE: LoginCompleteCommand,
    ResponseType.CLIENT_LOGIN: ClientLoginCommand,
}

def test_cmd_with_data():
    packed = Protocol.pack(cmd=NewUserCommand(
        public_port=CmdData.public_port))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == NewUserCommand.get_cmd_type()
    assert cast(NewUserCommand, cmd_instance).public_port == CmdData.public_port

    packed = Protocol.pack(cmd=RegisterCommand(
        channel_type=CmdData.channel_type,
        public_port=CmdData.public_port,
        service_id=CmdData.service_id))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == RegisterCommand.get_cmd_type()
    assert cast(RegisterCommand, cmd_instance).public_port == CmdData.public_port
    assert cast(RegisterCommand, cmd_instance).service_id == CmdData.service_id
    assert cast(RegisterCommand, cmd_instance).channel_type == CmdData.channel_type

    packed = Protocol.pack(cmd=RegisterCompleteCommand(
        public_port=CmdData.public_port,
        channel_type=CmdData.channel_type,
        service_id=CmdData.service_id))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == RegisterCompleteCommand.get_cmd_type()
    assert cast(RegisterCompleteCommand, cmd_instance).public_port == CmdData.public_port
    assert cast(RegisterCompleteCommand, cmd_instance).service_id == CmdData.service_id
    assert cast(RegisterCompleteCommand, cmd_instance).channel_type == CmdData.channel_type

    packed = Protocol.pack(cmd=ServiceBindCommand(
        channel_type=CmdData.channel_type,
        public_port=CmdData.public_port))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == ServiceBindCommand.get_cmd_type()
    assert cast(ServiceBindCommand, cmd_instance).public_port == CmdData.public_port
    assert cast(ServiceBindCommand, cmd_instance).channel_type == CmdData.channel_type

    packed = Protocol.pack(cmd=ReadyHolePunchingCommand(
        public_port=CmdData.public_port))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == ReadyHolePunchingCommand.get_cmd_type()
    assert cast(ReadyHolePunchingCommand, cmd_instance).public_port == CmdData.public_port

    packed = Protocol.pack(cmd=HolePunchingCommand(
        public_port=CmdData.public_port))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == HolePunchingCommand.get_cmd_type()
    assert cast(HolePunchingCommand, cmd_instance).public_port == CmdData.public_port

    packed = Protocol.pack(cmd=HolePunchingCompleteCommand(
        public_port=CmdData.public_port))
    cmd_type, cmd_instance = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])

    assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
        packed[Protocol.FRAME_HEADER.size:])
    assert cmd_type == HolePunchingCompleteCommand.get_cmd_type()
    assert cast(HolePunchingCompleteCommand, cmd_instance).public_port == CmdData.public_port


def test_cmd_without_data():
    for cmdtype, cmdcls in testCMD_without_data.items():
        packed = Protocol.pack(cmd=cmdcls())
        cmdtype_unpack, cmd_ins = Protocol.unpack(packed[Protocol.FRAME_HEADER.size:])
        assert int.from_bytes(packed[:Protocol.FRAME_HEADER.size], byteorder='big') == len(
            packed[Protocol.FRAME_HEADER.size:])
        assert cmdtype_unpack == cmdtype


def test_cmd_error():
    payload = {
        "cmd_type": "garbage",
        "data": {}
    }
    body = json.dumps(payload).encode()
    packed_error = Protocol.FRAME_HEADER.pack(len(body)) + body
    with pytest.raises(ValueError, match=f"bad cmd_type: {payload['cmd_type']}"):
        Protocol.unpack(packed_error[Protocol.FRAME_HEADER.size:])

    payload = {
        "cmd_type": "NEW_USER",
        "data": {"channel_type": CmdData.channel_type.value}
    }
    body = json.dumps(payload).encode()
    packed_error = Protocol.FRAME_HEADER.pack(len(body)) + body
    with pytest.raises(TypeError, match=f"miss match data {payload['data']} to cmd {payload['cmd_type']}"):
        Protocol.unpack(packed_error[Protocol.FRAME_HEADER.size:])


