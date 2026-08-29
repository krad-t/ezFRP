import socket
import selectors
from typing import cast

# socket.SOCK_DGRAM —— UDP
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 0))
print(f"====fake local app listening on {s.getsockname()[1]}===")
print("====             UDP                 ===")
sel = selectors.DefaultSelector()
sel.register(s, selectors.EVENT_READ,data=None)
while True:
    events = sel.select()
    for key, mask in events:
        if mask & selectors.EVENT_READ:
            sock = cast(socket.socket, key.fileobj)
            data, addr = sock.recvfrom(1024)
            print(f"fake local app received message from {addr}:{data.decode('utf-8')}")
            sock.sendto(data, addr)