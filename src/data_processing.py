# data_processing.py
import datetime
import json
import pytz
import xml.etree.ElementTree as ET

# 创建时区对象（协调世界时）
utc_tz = pytz.timezone('UTC')

# 缩写和数据库字段名的映射
field_mapping = {
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

def process_data(json_data, db_manager):
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
