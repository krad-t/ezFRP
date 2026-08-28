"""
ezFRP 文件发送端
用法: python file_sender.py <文件路径> <host> <port>
示例: python file_sender.py ./demo.pdf 124.221.101.254 9950
"""
import socket
import sys
import os


def send_file(file_path: str, host: str, port: int) -> bool:
    if not os.path.exists(file_path):
        print(f"[Sender] 错误: 文件不存在 - {file_path}")
        return False

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    if file_size == 0:
        print(f"[Sender] 错误: 文件为空")
        return False

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(30)
        sock.connect((host, port))
    except (ConnectionRefusedError, socket.timeout):
        print(f"[Sender] 错误: 无法连接到 {host}:{port}")
        return False

    try:
        # 协议头: [2字节文件名长度][文件名][8字节文件大小]
        name_bytes = file_name.encode("utf-8")
        header = len(name_bytes).to_bytes(2, "big") + name_bytes + file_size.to_bytes(8, "big")
        sock.sendall(header)

        print(f"[Sender] 发送: {file_name} ({file_size:,} bytes)")

        sent = 0
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                sock.sendall(chunk)
                sent += len(chunk)
                pct = sent * 100 // file_size
                print(f"\r[Sender] 进度: {sent:,}/{file_size:,} ({pct}%)", end="", flush=True)
        print()

        # 等接收端确认
        status = sock.recv(1)
        if status == b"\x00":
            print(f"[Sender] 传输成功 ✓")
            return True
        else:
            print(f"[Sender] 接收端报告失败")
            return False
    except (ConnectionResetError, socket.timeout) as e:
        print(f"\n[Sender] 传输中断: {e}")
        return False
    finally:
        sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("用法: python file_sender.py <文件路径> <host> <port>")
        print("示例: python file_sender.py ./demo.pdf 124.221.101.254 9950")
        sys.exit(1)

    file_path = sys.argv[1]
    host = sys.argv[2]
    try:
        port = int(sys.argv[3])
    except ValueError:
        print(f"错误: 无效端口号 - {sys.argv[3]}")
        sys.exit(1)

    if not (1 <= port <= 65535):
        print(f"错误: 端口超出范围 - {port}")
        sys.exit(1)

    success = send_file(file_path, host, port)
    sys.exit(0 if success else 1)
