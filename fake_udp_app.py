import socket

# socket.SOCK_DGRAM —— UDP
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 25565))
print("====fake local app listening on 25565===")
print("====             UDP                 ===")
while True:
    data, addr = s.recvfrom(1024)
    print(f"fake local app received message from {addr}:{data.decode('utf-8')}")
    s.sendto(data, addr)