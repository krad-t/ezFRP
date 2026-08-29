"""
ezFRP 文件接收端
用法: python file_receiver.py <监听端口> [输出目录]
示例: python file_receiver.py 5000 .
      在 5000 端口监听，接收文件保存到当前目录

注意: 请先启动 receiver，再启动 ezFRP Client。
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
    listen_sock.listen(5)  # 允许排队，应对 Client 同时来多个连接

    print(f"[Receiver] 等待 ezFRP 隧道建立... (监听端口 {listen_port})")

    while True:
        try:
            conn, addr = listen_sock.accept()
        except KeyboardInterrupt:
            print("\n[Receiver] 已取消")
            return False

        print(f"[Receiver] 新连接: {addr[0]}:{addr[1]}")

        try:
            conn.settimeout(300)
            success = _handle_one_file(conn, addr, output_dir)
        finally:
            conn.close()

        if success:
            listen_sock.close()
            return True
        # 不成功 → 这个连接是预连接/空闲/断开 → 继续 accept 下一个


def _handle_one_file(conn: socket.socket, addr: tuple, output_dir: str) -> bool:
    """处理单个 TCP 连接上的文件传输，成功返回 True"""
    # 读协议头: [2字节文件名长度]
    raw_len = _recv_exact(conn, 2)
    if not raw_len:
        # 预连接没数据就断了 → 正常，等下一个
        print(f"[Receiver] 预连接断开，等待正式传输...")
        return False
    name_len = int.from_bytes(raw_len, "big")
    if name_len > 1024:
        print(f"[Receiver] 错误: 文件名过长 ({name_len})")
        _safe_send(conn, b"\x01")
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

    # 防覆盖
    output_path = _unique_path(output_dir, file_name)

    print(f"[Receiver] 接收: {file_name} → {os.path.basename(output_path)} ({file_size:,} bytes)")

    # 读文件数据
    received = 0
    with open(output_path, "wb") as f:
        while received < file_size:
            want = min(65536, file_size - received)
            chunk = conn.recv(want)
            if not chunk:
                print(f"\n[Receiver] 错误: 在 {received:,}/{file_size:,} 处断开")
                _safe_send(conn, b"\x01")
                return False
            f.write(chunk)
            received += len(chunk)
            pct = received * 100 // file_size
            print(f"\r[Receiver] 进度: {received:,}/{file_size:,} ({pct}%)",
                  end="", flush=True)
    print()

    _safe_send(conn, b"\x00")
    print(f"[Receiver] 传输成功 ✓")
    return True


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """精确读取 n 字节，中途断开返回 None"""
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except ConnectionResetError:
            return None
        if not chunk:
            # 空 = 对方关闭，不打印错误（可能是预连接断开）
            return None
        buf += chunk
    return buf


def _safe_send(sock: socket.socket, data: bytes):
    try:
        sock.sendall(data)
    except OSError:
        pass


def _unique_path(output_dir: str, file_name: str) -> str:
    path = os.path.join(output_dir, file_name)
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    counter = 1
    while os.path.exists(f"{base}_{counter}{ext}"):
        counter += 1
    return f"{base}_{counter}{ext}"


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
