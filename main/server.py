# server.py
import socket
import json
import xml.etree.ElementTree as ET
from database import DatabaseManager, DatabaseError
import data_processing

def main():
    # 读取XML配置文件
    tree = ET.parse('main/config.xml')
    root = tree.getroot()

    # 获取服务器地址和端口号
    host = root.find('host').text
    port = int(root.find('port').text)

    # SQLite数据库文件路径
    db_file = root.find('db_file').text

    # 创建 Socket 对象
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 绑定服务器地址和端口号
    server_socket.bind((host, port))

    # 设置服务器等待客户端连接的超时时间为 10 秒
    server_socket.settimeout(10)

    # 监听连接
    server_socket.listen(5)
    print('Listening for connections...')

    try:
        # 连接 SQLite 数据库
        db_manager = DatabaseManager(db_file)
        db_manager.connect()

        # 创建数据表（如果不存在）
        db_manager.create_tables()
        
        while True:
            try:
                # 接收连接
                conn, addr = server_socket.accept()
                conn.settimeout(10)
                print('Connected by', addr)
        
                while True:
                    # 从客户端接收数据
                    data = conn.recv(1024).decode('utf-8')
                    print('Send from', addr, 'Received data:', data)
        
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
            except socket.timeout:
                print('超时。正在关闭连接。')
                conn.close()
            except json.decoder.JSONDecodeError:
                print('Invalid data received. Closing connection.')
                conn.close()
            except socket.error as e:
                print('Socket error:', str(e))
    except DatabaseError as e:
        print('Database error:', str(e))
    finally:
        # 关闭服务器
        if server_socket:
            server_socket.close()
        
        # 关闭数据库连接
        if db_manager and db_manager.conn:
            db_manager.close()

if __name__ == "__main__":
    main()
