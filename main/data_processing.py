# data_processing.py
import datetime
import pytz

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
    dt = datetime.datetime.fromtimestamp(int(values[0]) / 1000, tz=utc_tz)

    # 将 datetime 对象格式化为指定的字符串格式
    formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 构建插入查询语句
    fields = [field_mapping[abbr] for abbr in all_abbreviations]
    placeholders = ', '.join(['?'] * len(fields))
    insert_query = f"INSERT INTO data ({', '.join(fields)}) VALUES ({placeholders})"

    # 执行插入操作
    db_manager.cursor.execute(insert_query, (formatted_time, *values[1:]))
