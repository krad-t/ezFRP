import pytest

from core.ezfrp_server import Server


class AllocatorData:
    min_port = 9000
    max_port = 9010


def test_allocator():
    allocator = Server.PortAllocator(min_port=AllocatorData.min_port, max_port=AllocatorData.max_port)
    port_set = set()
    for i in range(AllocatorData.min_port, AllocatorData.max_port + 1):
        port = allocator.get_a_free_port()
        assert AllocatorData.min_port <= port <= AllocatorData.max_port
        assert port not in port_set
    with pytest.raises(IndexError, match="No available port"):
        allocator.get_a_free_port()

    allocator = Server.PortAllocator(min_port=AllocatorData.min_port, max_port=AllocatorData.max_port)
    for i in range(AllocatorData.min_port, AllocatorData.max_port):
        allocator.get_a_free_port()
        assert allocator.reserve(i) is False
    assert allocator.reserve(AllocatorData.max_port) is True
    assert allocator.reserve(AllocatorData.max_port) is False
    assert allocator.reserve(AllocatorData.max_port + 1) is False

    allocator.free_a_port(AllocatorData.min_port)
    assert allocator.is_used(AllocatorData.min_port) is False
    assert allocator.reserve(AllocatorData.min_port) is True
    assert allocator.is_used(AllocatorData.min_port) is True
    allocator.free_a_port(AllocatorData.min_port)
    assert allocator.is_used(AllocatorData.min_port) is False
    assert allocator.get_a_free_port() == AllocatorData.min_port
    assert allocator.is_used(AllocatorData.min_port) is True
    allocator.free_a_port(AllocatorData.min_port)
    assert allocator.is_used(AllocatorData.min_port) is False
    assert allocator._data == [9000]
    allocator.free_a_port(AllocatorData.min_port)
    assert allocator.is_used(AllocatorData.min_port) is False
    assert allocator._data == [9000]
    allocator.get_a_free_port()
    with pytest.raises(IndexError, match="No available port"):
        allocator.get_a_free_port()
