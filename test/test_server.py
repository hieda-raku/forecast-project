# 导入必要的库
import socket
import json
def server():
    """
    主函数，从XML文件中获取服务器和数据库配置，然后开启服务器监听客户端连接，处理和保存数据
    """

    # 获取服务器地址和端口号
    host = '0.0.0.0'
    port = 18120

    # 创建 Socket 对象以便接收客户端连接
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 将服务器地址和端口号绑定到 Socket 对象
    server_socket.bind((host, port))

    # 开始监听客户端的连接请求
    server_socket.listen(5)
    print('正在监听连接...')

    try:

        while True:
            try:
                # 接收客户端的连接请求
                conn, addr = server_socket.accept()
                conn.settimeout(30)
                print('连接来自', addr)

                while True:
                    # 从客户端接收数据
                    data = conn.recv(4096)
                    print(data)
                    umb_data = ' '.join([f'{byte:02X}' for byte in data])
                    byte_list = umb_data.split()
                    print(byte_list)

            except socket.timeout:
                print('超时。正在关闭连接。')
                conn.close()
            except json.decoder.JSONDecodeError:
                print('接收到无效数据。正在关闭连接。')
                conn.close()
            except socket.error as e:
                print('套接字错误:', str(e))
    finally:
        # 关闭服务器
        if server_socket:
            server_socket.close()

server()

