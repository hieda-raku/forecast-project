# 导入必要的库
import socket
import json
import xml.etree.ElementTree as ET

# 导入自定义库
from database import DatabaseManager, DatabaseError
import data_processing
from data_processing import process_station_data

def main():
    """
    主函数，从XML文件中获取服务器和数据库配置，然后开启服务器监听客户端连接，处理和保存数据
    """
    # 读取XML配置文件以获取服务器配置信息
    tree = ET.parse('config.xml')
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

                while True:
                    # 从客户端接收数据
                    data = conn.recv(1024).decode('utf-8')
                    print('发送自', addr, '接收到的数据:', data)

                    if not data:
                        print("客户端已关闭连接")
                        conn.close()
                        break
                    else:
                        # 将接收到的数据解析为 JSON 对象
                        json_data = json.loads(data)

                        # 处理接收到的数据
                        data_processing.process_data(json_data, db_manager)

                        # 提交事务，将数据保存到数据库
                        db_manager.commit()
                        print('数据已保存到数据库')

                        # 向客户端发送响应
                        response = '数据已成功保存'
                        conn.sendall(response.encode('utf-8'))
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

# 如果直接运行此脚本，则执行 main 函数
if __name__ == "__main__":
    main()
