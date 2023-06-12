import socket
import json
import xml.etree.ElementTree as ET
import data_processing
from data_processing import DatabaseManager

# 读取XML配置文件
tree = ET.parse('config.xml')
root = tree.getroot()

# 获取服务器地址和端口号
host = root.find('host').text
port = int(root.find('port').text)

# SQLite数据库文件路径
db_file = 'data.db'

# 创建 Socket 对象
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 绑定服务器地址和端口号
server_socket.bind((host, port))

# 监听连接
server_socket.listen(5)
print('Listening for connections...')

# 连接 SQLite 数据库
db_manager = DatabaseManager(db_file)
db_manager.connect()

# 创建数据表（如果不存在）
db_manager.create_tables()

while True:
    try:
        # 接收连接
        conn, addr = server_socket.accept()
        print('Connected by', addr)

        while True:
            # 从客户端接收数据
            data = conn.recv(1024).decode('utf-8')
            print('Send from', addr, 'Received data:', data)

            # 将接收到的数据解析为 JSON 对象
            json_data = json.loads(data)
        
            # 判断数据类型
            data_type = json_data.get('type')
            if data_type == 'forecast':
                data_processing.process_forecast_data(json_data, db_manager.cursor)
            elif data_type == 'observation':
                data_processing.process_observation_data(json_data, db_manager.cursor)

            # 提交事务
            db_manager.commit()
            print('Data saved to database')

            # 发送响应
            response = 'Data saved successfully'
            conn.sendall(response.encode('utf-8'))
    except json.decoder.JSONDecodeError:
        print('Invalid data received. Closing connection.')
        conn.close()
    except Exception as e:
        print('Error:', str(e))

# 关闭服务器
server_socket.close()

# 关闭数据库连接
db_manager.close()
