"""
ezFRP 文件接收端
用法: python file_receiver.py <监听端口> [输出目录]
示例: python file_receiver.py 5000 .
      在 5000 端口监听，接收文件保存到当前目录
"""
import socket
import sys
import os


def receive_file(listen_port: int, output_dir: str = ".") -> bool:
    if not os.path.isdir(output_dir):
        print(f"[Receiver] 错误: 输出目录不存在 - {output_dir}")
        return False

    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind(("0.0.0.0", listen_port))
    listen_sock.listen(1)

    print(f"[Receiver] 等待发送端连接... (监听端口 {listen_port})")

    try:
        conn, addr = listen_sock.accept()
    except KeyboardInterrupt:
        print("\n[Receiver] 已取消")
        return False

    print(f"[Receiver] 发送端已连接: {addr[0]}:{addr[1]}")

    try:
        conn.settimeout(30)

        # 读协议头: [2字节文件名长度]
        raw_len = _recv_exact(conn, 2)
        if not raw_len:
            print("[Receiver] 错误: 发送端断开")
            return False
        name_len = int.from_bytes(raw_len, "big")
        if name_len > 1024:
            print(f"[Receiver] 错误: 文件名过长 ({name_len})")
            conn.sendall(b"\x01")
            return False

        # 读文件名
        name_bytes = _recv_exact(conn, name_len)
        if not name_bytes:
            return False
        file_name = name_bytes.decode("utf-8")

        # 读文件大小
        raw_size = _recv_exact(conn, 8)
        if not raw_size:
            return False
        file_size = int.from_bytes(raw_size, "big")

        # 防止覆盖，重名加后缀
        output_path = os.path.join(output_dir, file_name)
        base, ext = os.path.splitext(output_path)
        counter = 1
        while os.path.exists(output_path):
            output_path = f"{base}_{counter}{ext}"
            counter += 1

        print(f"[Receiver] 接收: {file_name} → {output_path} ({file_size:,} bytes)")

        # 读文件数据
        received = 0
        with open(output_path, "wb") as f:
            while received < file_size:
                want = min(65536, file_size - received)
                chunk = conn.recv(want)
                if not chunk:
                    print(f"\n[Receiver] 错误: 发送端在 {received:,}/{file_size:,} 处断开")
                    conn.sendall(b"\x01")
                    return False
                f.write(chunk)
                received += len(chunk)
                pct = received * 100 // file_size
                print(f"\r[Receiver] 进度: {received:,}/{file_size:,} ({pct}%)",
                      end="", flush=True)
        print()

        conn.sendall(b"\x00")
        print(f"[Receiver] 传输成功 ✓")
        return True
    except socket.timeout:
        print(f"\n[Receiver] 错误: 接收超时")
        try:
            conn.sendall(b"\x01")
        except OSError:
            pass
        return False
    finally:
        conn.close()
        listen_sock.close()


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """精确读取 n 字节，中途断开返回 None"""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            print("[Receiver] 错误: 发送端意外断开")
            return None
        buf += chunk
    return buf


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("用法: python file_receiver.py <监听端口> [输出目录]")
        print("示例: python file_receiver.py 5000 .")
        sys.exit(1)

    try:
        port = int(sys.argv[1])
    except ValueError:
        print(f"错误: 无效端口号 - {sys.argv[1]}")
        sys.exit(1)

    output_dir = sys.argv[2] if len(sys.argv) == 3 else "."

    success = receive_file(port, output_dir)
    sys.exit(0 if success else 1)
