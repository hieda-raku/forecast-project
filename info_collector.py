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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    # 接受连接
    conn, addr = server_socket.accept()
    print('Connected by', addr)

    conn.setblocking(False)
    
    # 重置连接标志
    connected = True

    while connected:
        try:
            # 从客户端接收数据
            data = conn.recv(1024).decode('utf-8')

            # 检查是否收到空数据，表示客户端断开连接
            if not data:
                print('Client disconnected')
                connected = False
                break
            print('Received data:', data)


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
                solar_flux= json_data.get('sf')
                infra_red= json_data.get('ir')
                cloud_coverage = json_data.get('cc')

                forecast_time = forecast_time if forecast_time is not None else None
                air_temperature = air_temperature if air_temperature is not None else None
                dew_point = dew_point if dew_point is not None else None
                rainfall = rainfall if rainfall is not None else None
                snowfall = snowfall if snowfall is not None else None
                wind_speed = wind_speed if wind_speed is not None else None
                atmospheric_pressure = atmospheric_pressure if atmospheric_pressure is not None else None
                solar_flux = solar_flux if solar_flux is not None else None
                infra_red = infra_red if infra_red is not None else None
                cloud_coverage = cloud_coverage if cloud_coverage is not None else None

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

                observation_time = observation_time if observation_time is not None else None
                air_temperature = air_temperature if air_temperature is not None else None
                dew_point_temperature = dew_point_temperature if dew_point_temperature is not None else None
                precipitation = precipitation if precipitation is not None else None
                wind_speed = wind_speed if wind_speed is not None else None
                road_condition = road_condition if road_condition is not None else None
                road_temperature = road_temperature if road_temperature is not None else None
                sub_road_temperature = sub_road_temperature if sub_road_temperature is not None else None


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

        except socket.error:
            # 客户端断开连接，处理断开连接的逻辑
            print('Client disconnected')
            connected = False

# 关闭连接
conn.close()

# 关闭数据库连接
db_cursor.close()
db_connection.close()
