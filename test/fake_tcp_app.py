import socket
import selectors
from typing import cast


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
s.listen(5)
sel = selectors.DefaultSelector()
sel.register(s, selectors.EVENT_READ,data=1)
print(f"====fake local app listening on {s.getsockname()[1]}===")
print("====             TCP                 ===")

while True:
    events = sel.select()
    for key, mask in events:
        if mask & selectors.EVENT_READ:
            sock = cast(socket.socket, key.fileobj)
            data = int(key.data)
            if data == 1:
                tcp, _ = sock.accept()
                sel.register(tcp, selectors.EVENT_READ, data=2)
            elif data == 2:
                recv_data = sock.recv(1024)
                if recv_data:
                    print(f"received: {sock.getpeername()}-{recv_data}")
                    sock.send(recv_data)