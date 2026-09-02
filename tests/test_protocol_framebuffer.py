from typing import cast
import pytest

from core.protocol import *

@dataclass()
class CmdData:
    public_port: int = 9900
    channel_type: ResponseType = ResponseType.TCP
    service_id: int = 0

def test_single_frame():
    packed = Protocol.pack(cmd=NewUserCommand(
        public_port=CmdData.public_port))
    buffer = FrameBuffer()
    frames = buffer.feed(packed)
    assert len(frames) == 1
    assert cast(NewUserCommand, frames[0][1]).public_port == CmdData.public_port

def test_multiple_frames():
    packed_1 = Protocol.pack(cmd=NewUserCommand(
        public_port=CmdData.public_port
    ))
    packed_2 = Protocol.pack(cmd=RegisterCommand(
        channel_type=CmdData.channel_type,
        public_port=CmdData.public_port,
        service_id=CmdData.service_id,
    ))

    buffer = FrameBuffer()
    frames = buffer.feed(packed_1 + packed_2)
    # 同时feed两个frame，应能解析出两个
    assert len(frames) == 2
    assert frames[0][0] == ResponseType.NEW_USER
    assert frames[1][0] == ResponseType.REGISTER
    assert cast(NewUserCommand, frames[0][1]).public_port == CmdData.public_port
    assert cast(RegisterCommand, frames[1][1]).channel_type == CmdData.channel_type
    assert cast(RegisterCommand, frames[1][1]).public_port == CmdData.public_port
    assert cast(RegisterCommand, frames[1][1]).service_id == CmdData.service_id

def test_feed_skips_bad_frame():
    packed_correct = Protocol.pack(cmd=NewUserCommand(public_port=CmdData.public_port))
    payload = {
        "cmd_type": "garbage",
        "data": {}
    }
    body = json.dumps(payload).encode()
    packed_error = Protocol.FRAME_HEADER.pack(len(body)) + body

    buffer = FrameBuffer()
    frames = buffer.feed(packed_error)
    assert len(frames) == 0
    assert len(buffer.drop) == 1
    assert buffer.drop[0][0] == f"bad cmd_type: {payload['cmd_type']}"
    assert buffer.drop[0][1] == packed_error[4:64]
    buffer = FrameBuffer()
    frames = buffer.feed(packed_correct)
    assert len(frames) == 1
    assert cast(NewUserCommand, frames[0][1]).public_port == CmdData.public_port

    buffer = FrameBuffer()
    frames = buffer.feed(packed_correct + packed_error)
    assert len(frames) == 1
    assert buffer.drop[0][0] == f"bad cmd_type: {payload['cmd_type']}"
    assert buffer.drop[0][1] == packed_error[4:64]
    assert cast(NewUserCommand, frames[0][1]).public_port == CmdData.public_port



def test_half_frames():
    packed = Protocol.pack(cmd=NewUserCommand(
        public_port=CmdData.public_port
    ))
    packed_1 = packed[:len(packed)//2]
    packed_2 = packed[len(packed)//2:]

    buffer = FrameBuffer()
    frames = buffer.feed(packed_1)
    # 只有一半frame 应解析出空列表
    assert len(frames) == 0

    frames = buffer.feed(packed_2)
    # 同一个frame 剩下部分feed后应能解析出
    assert len(frames) == 1
    assert cast(NewUserCommand, frames[0][1]).public_port == CmdData.public_port

    packed_1 = packed[:Protocol.FRAME_HEADER.size//2]
    packed_2 = packed[Protocol.FRAME_HEADER.size//2:]
    buffer = FrameBuffer()
    frames = buffer.feed(packed_1)
    assert len(frames) == 0
    frames = buffer.feed(packed_2)
    assert len(frames) == 1
    assert cast(NewUserCommand, frames[0][1]).public_port == CmdData.public_port

def test_error_frames():
    packed = bytes.fromhex("00000000") + b"\x00" * 10

    with pytest.raises(ValueError, match=f"bad frame length {0}"):
        buffer = FrameBuffer()
        frames = buffer.feed(packed)

    packed = (Protocol.MAX_FRAME_SIZE + 1).to_bytes(byteorder="big",length=Protocol.FRAME_HEADER.size) + b"\x00" * 10

    with pytest.raises(ValueError, match=f"bad frame length {Protocol.MAX_FRAME_SIZE + 1}"):
        buffer = FrameBuffer()
        buffer.feed(packed)


