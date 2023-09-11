# data_processing.py
from datetime import datetime, timezone
import json
import pytz
import struct
import xml.etree.ElementTree as ET
from database import DatabaseManager, DatabaseError

# 定义全局变量
data_list = []
last_device_id = None

# 创建北京时区对象
beijing_tz = pytz.timezone('Asia/Shanghai')

road_condition_mapping = {
    10: 33,  # 干
    15: 34,  # 潮
    20: 34,  # 湿
    25: 34,  # 潮
    30: 34,  # 湿
    35: 35,  # 冰
    40: 35,  # 雪
    45: 40,  # 霜
}

#UMB通道对应值
channel_to_field = {
    7:
        {
            100: 'air_temperature',
            110: 'dewpoint_temperature',
            200: 'humidity',
            300: 'atmospheric_pressure',
            401: 'wind_speed',
            501: 'wind_direction',
            620: 'absolute_precipitation',
            625: 'realitive_precipitation',
            820: 'rainfall_intensity',
        },
    9:
        {
            101: 'road_surface_temperature',
            151: 'freezing_point',
            601: 'water_film_height',
            801: 'saline_concent',
            810: 'ice_percentage',
            820: 'friction',
            900: 'road_condition',
        }
}

#roadcast对应值
field_mapping = {
    'roadcast-time' : 'forecast_time',
    'hh' : 'hour_after',
    'st' : 'road_surface_temperature',
    'sst' : 'road_subsurface_temperature',
    'at' : 'air_temperature',
    'td' : 'dew_point',
    'ws' : 'wind_speed',
    'sn' : 'ice_quantity',
    'ra' : 'rain_quantity',
    'qp-sn' : 'total_snow_precipitation',
    'qp-ra' : 'total_rain_precipitation',
    'sf' : 'solar_flux',
    'ir' : 'infra_red_flux',
    'fv' : 'vapor_flux',
    'fc' : 'sensible_heat',
    'fa' : 'anthropogenic_flux',
    'fg' : 'ground_exchange_flux',
    'bb' : 'blackbody_effect',
    'fp' : 'phase_change',
    'rc' : 'road_condition'
}

def process_umb_data(hex_data,db_manager,registered_station_id):
    global data_list
    global last_device_id

    # 将十六进制数据拆分成单个字节
    byte_list = hex_data.split()

    # 提取前十二位数据存入front_data
    front_data = byte_list[:12]

    # 获取通道数
    cannel_number = int(front_data[11], 16)

    # 获取新的设备ID
    new_device_id = int(front_data[5][0],16)
    # 移除前12位
    byte_list = byte_list[12:]

    # 将最后四位存入end_data并移除之
    end_data = byte_list[-4:]
    byte_list = byte_list[:-4]

    # 循环处理byte_list里剩余的的数据组
    # 通道数代表需要循环的次数
    for _ in range(cannel_number):

        # 列表里第一位永远是本组数据的长度,将其转换为10进制
        datalan = int(byte_list[0], 16)

        # 为了保证后续处理将其弹出
        byte_list = byte_list[1:]

        # 切片数据长度位的数据组进入rawdata以供后续处理
        rawdata = byte_list[0:datalan]

        # 第一位是错误码
        errorcode = rawdata[0]

        # 第二三位转成10进制后是通道号
        channel_dex = int(''.join(rawdata[1:3][::-1]), 16)

        # 通过错误码判断是否报错
        if errorcode == '00':

            # 删除前三位
            del rawdata[0:3]

            # 第四位是数据类型
            datatype = rawdata[0]

            # 为了后续处理，同样弹出
            rawdata.pop(0)

            '''
                判断数据类型，如果是一位，则不需要改变排序，
                如果是一位以上，则需要反转列表再转化为目标类型数据，
                目前只有浮点数，先只处理为浮点数            
            '''
            if datatype == '11' or datatype == '10':
                field_name = channel_to_field[new_device_id].get(channel_dex, None)
                value_dex = road_condition_mapping[int(rawdata[0], 16)]
                data_list.append({field_name: value_dex})
            else:
                value_hex = ''.join(rawdata[::-1])
                field_name = channel_to_field[new_device_id].get(channel_dex, None)
                value_dex = round(struct.unpack('!f', bytes.fromhex(value_hex))[0], 2)
                data_list.append({field_name: value_dex})

        # 如果报错则直接输出错误码
        else:
            errorcode = int(errorcode, 16)
            field_name = channel_to_field[new_device_id].get(channel_dex, None)
            data_list.append({field_name : 9999})

        # 从byte_list里弹出处理完的数据组，进入下一个循环处理下一个数据组
        del byte_list[0:datalan]

    if new_device_id == 9 and last_device_id == 7:
        # 转换数据列表为字典
        data_list = {k: v for item in data_list for k, v in item.items()}
        # 添加 station_id 到值列表中
        data_list['station_id']  = registered_station_id

        # 获取当前UTC时间，并设置时区信息
        current_utc_time = datetime.now(timezone.utc)

        # 使用isoformat方法将时间格式化为ISO 8601格式，自动添加“Z”后缀
        current_time_str = current_utc_time.isoformat()
        data_list['data_time'] = current_time_str
        # 构建插入查询语句
        fields = ', '.join(data_list.keys())
        placeholders = ', '.join(['?'] * len(data_list))
        insert_query = f"INSERT INTO observation ({fields}) VALUES ({placeholders})"

        # 执行插入操作
        db_manager.cursor.execute(insert_query, list(data_list.values()))
        # 提交事务
        db_manager.commit()
        # 清空数据列表以便存储下一对数据
        data_list = []
    elif new_device_id ==  last_device_id :
        # 清空错误数据列表以便存储下一对数据
        data_list = []

    # 将新的设备ID设置为上一次的设备ID
    last_device_id = new_device_id

def process_roadcast(xml_data,db_file):

    # 读取XML文件
    with open(xml_data, 'r', encoding='utf-8') as file:
        xml_data = file.read()

    # 解析XML
    try:
        tree = ET.ElementTree(ET.fromstring(xml_data))
    except ET.ParseError as e:
        print(f"Parse error: {e}")
        return
    
    db_manager = DatabaseManager(db_file)
    db_manager.connect()
    db_manager.create_tables()  # 如果还没有创建表

    tree = ET.ElementTree(ET.fromstring(xml_data))
    root = tree.getroot()

    for prediction in tree.findall('.//prediction'):
        prediction_data = {}
        for xml_key, mapped_key in field_mapping.items():
            element = prediction.find(xml_key)
            if element is not None:
                prediction_data[mapped_key] = float(element.text) if xml_key != 'roadcast-time' else element.text
            else:
                print(f"Element not found for key: {xml_key}")

        # 在循环结束后构造查询并执行一次插入操作
        columns = ', '.join(prediction_data.keys())
        placeholders = ', '.join('?' * len(prediction_data))
        query = f'INSERT INTO roadcast ({columns}) VALUES ({placeholders})'
        db_manager.cursor.execute(query, tuple(prediction_data.values()))

    print('存储完成')    
    db_manager.commit()
    db_manager.close()

def process_json_data(json_data, db_manager):
    # 提取所有可能的缩写
    all_abbreviations = list(field_mapping.keys())

    # 提取接收到的字段值，如果字段没有在接收的数据中指定，那么将其值设为'9999'
    values = [json_data.get(abbr, '9999') for abbr in all_abbreviations]

    # 将时间戳转换为 datetime 对象，并设置时区为协调世界时 (UTC)
    dt = datetime.datetime.fromtimestamp(int(values[0]), tz=utc_tz)

    # 将 datetime 对象格式化为指定的字符串格式
    formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 构建插入查询语句
    fields = [field_mapping[abbr] for abbr in all_abbreviations]
    placeholders = ', '.join(['?'] * len(fields))
    insert_query = f"INSERT INTO data ({', '.join(fields)}) VALUES ({placeholders})"

    # 执行插入操作
    db_manager.cursor.execute(insert_query, (formatted_time, *values[1:]))

def process_station_data(xml_file, db_manager):
    # 读取并解析 XML 文件
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # 提取所需的数值
    road_station = root.find('header/road_station').text
    station_city = root.find('header/station_city').text
    production_date = root.find('header/production_date').text
    latitude = float(root.find('header/coordinate/latitude').text)
    longitude = float(root.find('header/coordinate/longitude').text)
    station_type = root.find('header/station_type').text

    roadlayers = root.findall('roadlayer_list/roadlayer')
    road_layers_data = []

    for roadlayer in roadlayers:
        position = int(roadlayer.find('position').text)
        layer_type = roadlayer.find('type').text
        thickness = float(roadlayer.find('thickness').text)

        # 构建 roadlayer 数据
        road_layer_data = {
            'position': position,
            'type': layer_type,
            'thickness': thickness
        }

        road_layers_data.append(road_layer_data)

    # 转换为 JSON 字符串
    road_layers_json = json.dumps(road_layers_data)

    # 检查数据库中是否已存在具有相同 road_station 的记录
    existing_record = db_manager.cursor.execute("SELECT * FROM stations WHERE station_id = ?", (road_station,)).fetchone()
    if existing_record:
        print(f"站点 {road_station} 的记录已存在，跳过插入操作")
        return

    # 将数据插入数据库
    db_manager.cursor.execute("INSERT INTO stations (station_id,station_city,production_date, latitude, longitude, station_type, road_layers) VALUES (?, ?, ?, ?, ?, ?,?)",
                              (road_station, station_city,production_date, latitude, longitude, station_type, road_layers_json))
    db_manager.commit()

    print(f"站点 {road_station} 的记录已成功插入数据库")
