import socket
import json
import sqlite3
import datetime
import pytz
import xml.etree.ElementTree as ET

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
db_connection = sqlite3.connect(db_file)
db_cursor = db_connection.cursor()

# 创建时区对象（协调世界时）
utc_tz = pytz.timezone('UTC')

# 创建数据表（如果不存在）
create_table_query = '''
CREATE TABLE IF NOT EXISTS forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    forecast_time TEXT,
    air_temperature REAL,
    dew_point REAL,
    rainfall REAL,
    snowfall REAL,
    wind_speed REAL,
    atmospheric_pressure REAL,
    cloud_coverage REAL
);
CREATE TABLE IF NOT EXISTS observation (
    id INTEGER PRIMARY KEY,
    observation_time TEXT,
    air_temperature REAL,
    dew_point_temperature REAL,
    precipitation INTEGER,
    wind_speed REAL,
    road_condition TEXT,
    road_temperature REAL,
    sub_road_temperature REAL
);
'''
db_cursor.executescript(create_table_query)

# 接收连接
conn, addr = server_socket.accept()
print('Connected by', addr)

# 服务器循环
while True:

    # 从客户端接收数据
    data = conn.recv(1024).decode('utf-8')
    print('Send from', addr,'Received data:', data)

    # 将接收到的数据解析为 JSON 对象
    json_data = json.loads(data)

    # 判断数据类型
    data_type = json_data.get('type')
    if data_type == 'forecast':
        # 提取字段值
        forecast_time = json_data.get('forecast_time')
        air_temperature = json_data.get('at')
        dew_point = json_data.get('td')
        rainfall = json_data.get('ra')
        snowfall = json_data.get('sn')
        wind_speed = json_data.get('ws')
        atmospheric_pressure = json_data.get('ap')
        cloud_coverage = json_data.get('cc')

        # 将时间戳转换为 datetime 对象，并设置时区为协调世界时 (UTC)
        dt = datetime.datetime.fromtimestamp(forecast_time / 1000, tz=utc_tz)

        # 将 datetime 对象格式化为指定的字符串格式
        formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # 将数据插入到数据库中
        insert_query = "INSERT INTO forecast (forecast_time, air_temperature, dew_point, rainfall, snowfall, wind_speed, atmospheric_pressure, cloud_coverage) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        db_cursor.execute(insert_query, (formatted_time, air_temperature, dew_point, rainfall, snowfall, wind_speed, atmospheric_pressure, cloud_coverage))

    elif data_type == 'observation':
        # 提取字段值
        observation_time = json_data.get('observation_time')
        air_temperature = json_data.get('at')
        dew_point_temperature = json_data.get('td')
        precipitation = json_data.get('pi')
        wind_speed = json_data.get('ws')
        road_condition = json_data.get('sc')
        road_temperature = json_data.get('st')
        sub_road_temperature = json_data.get('sst')

        # 将时间戳转换为 datetime 对象，并设置时区为协调世界时 (UTC)
        dt = datetime.datetime.fromtimestamp(observation_time / 1000, tz=utc_tz)

        # 将 datetime 对象格式化为指定的字符串格式
        formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # 将数据插入到数据库中
        insert_query = "INSERT INTO observation (observation_time, air_temperature, dew_point_temperature, precipitation, wind_speed, road_condition, road_temperature, sub_road_temperature) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        db_cursor.execute(insert_query, (formatted_time, air_temperature, dew_point_temperature, precipitation, wind_speed, road_condition, road_temperature, sub_road_temperature))

    # 提交事务
    db_connection.commit()
    print('Data saved to database')

    # 发送响应
    response = 'Data saved successfully'
    conn.sendall(response.encode('utf-8'))

# 关闭连接
conn.close()

# 关闭数据库连接
db_cursor.close()
db_connection.close()
