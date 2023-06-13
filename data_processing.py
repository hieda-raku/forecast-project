import datetime
import sqlite3
import pytz

# 创建时区对象（协调世界时）
utc_tz = pytz.timezone('UTC')

class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None

    def create_tables(self):
        # 创建数据表的逻辑
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
            cloud_coverage REAL,
            solar_flux REAL,
            infrared_flux REAL
            
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
        self.cursor.executescript(create_table_query)
        self.conn.commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def process_data(json_data, db_cursor, table_name, field_mapping):
    # 提取字段值
    values = []
    for field, column in field_mapping.items():
        value = json_data.get(field)
        if value is None:
            value = 9999
        values.append(value)

    # 将时间戳转换为 datetime 对象，并设置时区为协调世界时 (UTC)
    dt = datetime.datetime.fromtimestamp(values[0] / 1000, tz=utc_tz)

    # 将 datetime 对象格式化为指定的字符串格式
    formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 构建插入查询语句
    insert_query = f"INSERT INTO {table_name} ({', '.join(field_mapping.values())}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    # 执行插入操作
    db_cursor.execute(insert_query, (formatted_time, *values[1:]))

# 在 process_forecast_data 函数中调用辅助函数
def process_forecast_data(json_data, db_cursor):
    field_mapping = {
        'forecast_time': 'forecast_time',
        'at': 'air_temperature',
        'td': 'dew_point',
        'ra': 'rainfall',
        'sn': 'snowfall',
        'ws': 'wind_speed',
        'ap': 'atmospheric_pressure',
        'cc': 'cloud_coverage',
        'sf': 'solar_flux',
        'ir': 'infrared_flux'
    }
    process_data(json_data, db_cursor, 'forecast', field_mapping)

# 在 process_observation_data 函数中调用辅助函数
def process_observation_data(json_data, db_cursor):
    field_mapping = {
        'observation_time': 'observation_time',
        'at': 'air_temperature',
        'td': 'dew_point_temperature',
        'pi': 'precipitation',
        'ws': 'wind_speed',
        'sc': 'road_condition',
        'st': 'road_temperature',
        'sst': 'sub_road_temperature'
    }
    process_data(json_data, db_cursor, 'observation', field_mapping)

