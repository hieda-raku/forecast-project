# 导入必要的库
import base64
import socket
import json
import xml.etree.ElementTree as ET

# 导入自定义库
from database import DatabaseManager, DatabaseError
from data_processing import process_station_data
from data_processing import process_umb_data

def server(db_lock):
    """
    主函数，从XML文件中获取服务器和数据库配置，然后开启服务器监听客户端连接，处理和保存数据
    """
    # 读取XML配置文件以获取服务器配置信息
    tree = ET.parse('forecast-project/config.xml')
    root = tree.getroot()

    # 获取服务器地址和端口号
    host = root.find('host').text
    port = int(root.find('port').text)

    # 获取SQLite数据库文件路径
    db_file = root.find('db_file').text

    # 创建 Socket 对象以便接收客户端连接
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 将服务器地址和端口号绑定到 Socket 对象
    server_socket.bind((host, port))

    # 开始监听客户端的连接请求
    server_socket.listen(5)
    print('正在监听连接...')

    db_manager = None  # 初始化数据库管理对象为 None

    try:
        # 连接 SQLite 数据库
        db_manager = DatabaseManager(db_file)
        db_manager.connect()

        # 如果数据库中不存在表，则创建表
        db_manager.create_tables()
        
        xml_file = 'station.xml'
        process_station_data(xml_file, db_manager)

        while True:
            try:
                # 接收客户端的连接请求
                conn, addr = server_socket.accept()
                conn.settimeout(30)
                print('连接来自', addr)
                # 接收一次性注册包               
                registration_data = conn.recv(1024)
                registered_station_id = registration_data.decode('utf-8')
                # 初始化消息缓冲区和标志变量
                message_buffer = b''
                has_start = False

                while True:
                    # 从客户端接收数据
                    data = conn.recv(4096)
                    if not data:
                        print("客户端已关闭连接")
                        conn.close()
                        break
                    elif data.startswith(b'\x01') and data.endswith(b'\x04'):
                        # 同时满足以 b'\x01' 开头和以 b'\x04' 结尾的数据
                        # 处理这种数据
                        umb_data = ' '.join([f'{byte:02X}' for byte in data])
                        with db_lock:
                            # 处理接收到的数据
                            process_umb_data(umb_data, db_manager,registered_station_id)
                            # 提交事务，将数据保存到数据库
                            db_manager.commit()
                    elif data.startswith(b'\x01'):
                        # 以 b'\x01' 开头的数据，保存到消息缓冲区，并将标志变量设置为 True
                        message_buffer = data
                        has_start = True
                    elif data.endswith(b'\x04') and has_start:
                        # 以 b'\x04' 结尾的数据，拼接到消息缓冲区，并将标志变量设置为 False
                        message_buffer += data
                        umb_data = ' '.join([f'{byte:02X}' for byte in message_buffer])
                        # 处理接收到的数据
                        with db_lock:
                            process_umb_data(umb_data, db_manager,registered_station_id)
                            # 提交事务，将数据保存到数据库
                            db_manager.commit()
                        # 清空消息缓冲区和标志变量
                        message_buffer = b''
                        has_start = False
                    else:
                        print("未被识别的数据，已丢弃")
                            # 处理可能出现的异常
            except socket.timeout:
                print('超时。正在关闭连接。')
                conn.close()
            except json.decoder.JSONDecodeError:
                print('接收到无效数据。正在关闭连接。')
                conn.close()
            except socket.error as e:
                print('套接字错误:', str(e))
    except DatabaseError as e:
        print('数据库错误:', str(e))
    finally:
        # 关闭服务器
        if server_socket:
            server_socket.close()

        # 关闭数据库连接
        if db_manager and db_manager.conn:
            db_manager.close()
