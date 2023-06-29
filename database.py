import sqlite3

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
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_time TEXT,
            air_temperature REAL,
            dew_point REAL,
            rainfall REAL,
            snowfall REAL,
            wind_speed REAL,
            atmospheric_pressure REAL,
            cloud_coverage INTEGER,
            solar_flux REAL,
            infrared_flux REAL,
            precipitation INTEGER,
            road_condition TEXT,
            road_surface_temperature REAL,
            road_subsurface_temperature REAL
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
