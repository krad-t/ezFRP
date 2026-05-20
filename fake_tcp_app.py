import socket
import threading

def handle_client(conn, addr):
    print(f"connected from {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        print(f"received: {conn}-{data}")
        conn.sendall(b"response from fake app: " + data)
    conn.close()
    print(f"disconnected: {addr}")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 25565))
s.listen(5)
print("====fake local app listening on 25565===")
print("====             TCP                 ===")

while True:
    conn, addr = s.accept()
    threading.Thread(target=handle_client, args=(conn, addr)).start()