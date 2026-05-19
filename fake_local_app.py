import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 25565))
s.listen(5)
print("fake local app listening on 25565")

while True:
  conn, addr = s.accept()
  print(f"connected from {addr}")
  while True:
      # conn.send(b'hello')
      data = conn.recv(1024)
      if not data:
          break
      print(f"received: {data}")
      conn.sendall(b"response from fake app, i received your massage and return it to you:" + data)
  conn.close()