# data_processing.py
import datetime
import json
import pytz
import struct
import xml.etree.ElementTree as ET

# 定义全局变量
data_list = []
last_device_id = None

# 创建时区对象（协调世界时）
utc_tz = pytz.timezone('UTC')

road_condition_mapping = {
    10: 1,  # 干
    15: 2,  # 潮
    20: 2,  # 湿
    25: 2,  # 潮
    30: 2,  # 湿
    35: 3,  # 冰
    40: 3,  # 雪
    45: 7,  # 霜
}

#UMB通道对应值
channel_to_field = {
    100: 'air_temperature',
    110: 'dew_point',
    300: 'atmospheric_pressure',
    401: 'wind_speed',
    820: 'rainfall',
    101: 'road_surface_temperature',
    900: 'road_condition',
}

# 缩写和数据库字段名的映射
field_mapping = {
    'station_id': 'station_id',
    'data_time': 'data_time',
    'at': 'air_temperature',
    'td': 'dew_point',
    'ra': 'rainfall',
    'sn': 'snowfall',
    'ws': 'wind_speed',
    'ap': 'atmospheric_pressure',
    'cc': 'cloud_coverage',
    'sf': 'solar_flux',
    'if': 'infrared_flux',
    'pi': 'precipitation',
    'sc': 'road_condition',
    'st': 'road_surface_temperature',
    'sst': 'road_subsurface_temperature'
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
    new_device_id = front_data[5][0]

    # 删除前12位
    del byte_list[:12]

    # 将最后四位存入end_data并删除之
    end_data = byte_list[-4:]
    del byte_list[-4:]

    # 循环处理byte_list里剩余的的数据组
    # 通道数代表需要循环的次数
    for _ in range(cannel_number):

        # 列表里第一位永远是本组数据的长度,将其转换为10进制
        datalan = int(byte_list[0], 16)

        # 为了保证后续处理将其弹出
        byte_list.pop(0)

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
                目前只有浮点数，先只处理为浮点数            '''
            if datatype == '11' or datatype == '10':
                value_dex = int(rawdata[0], 16)
                data_list.append({'channel': channel_dex, 'value': value_dex})
            else:
                value_hex = ''.join(rawdata[::-1])
                value_dex = round(struct.unpack('!f', bytes.fromhex(value_hex))[0], 2)
                data_list.append({'channel': channel_dex, 'value': value_dex})

        # 如果报错则直接输出错误码
        else:
            errorcode = int(errorcode, 16)
            data_list.append({'channel': channel_dex, 'errorcode': errorcode})

        # 从byte_list里弹出处理完的数据组，进入下一个循环处理下一个数据组
        del byte_list[0:datalan]

    if new_device_id == '9' and last_device_id == '7':
        for data in data_list:
            channel = data['channel']
            value = data.get('value')
            errorcode = data.get('errorcode')

            # 检查是否存在错误码
            if errorcode is not None:
                # 如果存在错误码，那么将 'value' 的值设置为 9999
                data['value'] = 9999
                continue  # 跳过当前循环

            # 检查通道是否在字典中
            if channel in channel_to_field:
                # 如果通道在字典中，那么处理并存储这个通道的数据
                field_name = channel_to_field[channel]
                if channel == 900:
                    # 如果通道是 900（路面状况），那么使用映射来转换值
                    value = road_condition_mapping.get(value, value)

                # 将处理后的数据添加到列表中
                data[field_name] = value
                
        # 提取所有可能的缩写
        all_abbreviations = list(field_mapping.keys())

        # 创建一个与 all_abbreviations 长度相同的 values 列表，并初始化为 None
        values = [None] * len(all_abbreviations)

        # 添加 station_id 到值列表中
        values[all_abbreviations.index('station_id')] = registered_station_id

        # 添加 data_time 到值列表中
        current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        values[all_abbreviations.index('data_time')] = current_time
        
        # 提取接收到的字段值，如果通道没有在接收的数据中指定，那么将其值设为 '9999'
        for abbr in all_abbreviations:
            if abbr != 'data_time' and abbr != 'station_id':
                field_name = field_mapping[abbr]
                data = next((data for data in data_list if field_name in data), None)
                if data:
                    values[all_abbreviations.index(abbr)] = data[field_name]
                else:
                    values[all_abbreviations.index(abbr)] = '9999'

        # 构建插入查询语句
        fields = ', '.join([field_mapping[abbr] for abbr in all_abbreviations])
        placeholders = ', '.join(['?'] * len(all_abbreviations))
        insert_query = f"INSERT INTO data ({fields}) VALUES ({placeholders})"

        # 执行插入操作
        db_manager.cursor.execute(insert_query, values)
        # 提交事务
        db_manager.commit()

        # 清空数据列表以便存储下一对数据
        data_list = []
    elif new_device_id ==  last_device_id :
        # 清空数据列表以便存储下一对数据
        data_list = []

    # 将新的设备ID设置为上一次的设备ID
    last_device_id = new_device_id


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
    road_station = root.find('header/road-station').text
    production_date = root.find('header/production-date').text
    latitude = float(root.find('header/coordinate/latitude').text)
    longitude = float(root.find('header/coordinate/longitude').text)
    station_type = root.find('header/station-type').text

    roadlayers = root.findall('roadlayer-list/roadlayer')
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
    db_manager.cursor.execute("INSERT INTO stations (station_id, production_date, latitude, longitude, station_type, road_layers) VALUES (?, ?, ?, ?, ?, ?)",
                              (road_station, production_date, latitude, longitude, station_type, road_layers_json))
    db_manager.commit()

    print(f"站点 {road_station} 的记录已成功插入数据库")
