from datetime import datetime
import socket
import threading

from protocol import SERVER_IP,CONTROL_PORT


class Client:
    def __init__(self):
        self.server_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_control.connect((SERVER_IP, CONTROL_PORT))
        self.control_thread = threading.Thread(target=self.handle_control, daemon=True)
        self.control_thread.start()


    def handle_control(self):
        print("Successfully connected to server", SERVER_IP)
        while True:
            choice = input("Establishing TCP(1) or UDP(2) tunnel:")
            if choice == '1':
                self.server_control.send(bytes('TCP',"utf-8"))
                # daemon继承
                t1 = threading.Thread(target=self.handle_control_tcp)
                t1.start()
                break
            elif choice == '2':
                self.server_control.send(bytes('UDP',"utf-8"))
                t1 = threading.Thread(target=self.handle_control_udp)
                t1.start()
                break


    def handle_control_udp(self):
        # udp
        while True:
            pass

    def handle_control_tcp(self):
        # tcp
        # 第一次是发送控制消息new，此消息表示"有新的外部用户连接了，client可以创建data socket了"
        # 只接受一个new指令
        while True:
            cmd = self.server_control.recv(1024).decode('utf-8')
            if cmd == 'new':
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}:{cmd}")
                # 这个server_data是新建立出来的,用于数据转发
                server_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_data.connect((SERVER_IP, CONTROL_PORT))
                # 一对 [server_data --- local_data] ,local_data是本地的内存对象实例，server_data是另一侧Server端的抽象代表

                # 这个local_data是连接本地的软件（例如MC服务端）端口的，每个外部用户必须得配备一个新的Socket
                local_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                local_data.connect(('127.0.0.1', 25565))

                def data_trans(socketA: socket.socket, socketB: socket.socket):
                    try:
                        while True:
                            data = socketA.recv(1024)
                            if not data:
                                socketB.close()
                                break
                            socketB.send(data)
                    except (ConnectionResetError, OSError):
                        pass
                    finally:
                        socketA.close()
                        socketB.close()

                threading.Thread(target=data_trans, args=(server_data, local_data)).start()
                threading.Thread(target=data_trans, args=(local_data, server_data)).start()

    def send_cmd(self, param):
        pass


if __name__ == '__main__':
    client = Client()
    client.control_thread.join()
    while True:
        cli_cmd = input("q to quit:")
        if cli_cmd == 'q':
            break
