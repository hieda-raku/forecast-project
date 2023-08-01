# database.py
import sqlite3

class DatabaseError(Exception):
    pass

class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.cursor = self.conn.cursor()
            # Enable foreign key support
            self.cursor.execute("PRAGMA foreign_keys = ON;")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None


    def create_tables(self):
        try:
            # 创建数据表的逻辑
            create_table_query = '''
                CREATE TABLE IF NOT EXISTS stations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT UNIQUE,
                    station_city TEXT,
                    production_date TEXT,
                    latitude REAL,
                    longitude REAL,
                    station_type TEXT CHECK(station_type IN('road','bridge')),
                    road_layers TEXT
                );
                CREATE TABLE IF NOT EXISTS observation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT,
                    data_time TEXT,
                    air_temperature REAL,
                    dewpoint_temperature REAL,
                    humidity REAL,
                    atmospheric_pressure REAL,
                    wind_speed REAL,
                    wind_direction REAL,
                    absolute_precipitation REAL,
                    realitive_precipitation REAL,
                    rainfall_intensity REAL,
                    road_surface_temperature REAL,
                    freezing_point REAL,
                    water_film_height REAL,
                    saline_concent REAL,
                    ice_percentage REAL,
                    friction REAL,
                    road_condition TEXT,
                    FOREIGN KEY (station_id) REFERENCES stations (station_id)
                );
                CREATE TABLE IF NOT EXISTS forecast (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    update_time TEXT,
                    forecast_time TEXT,
                    data_time TEXT,
                    forecast_city TEXT,
                    weather_code TEXT,
                    forecast_temperature REAL,
                    wind_speed REAL,
                    wind_direction REAL,
                    relative_humidity REAL,
                    precipitation REAL,
                    atmospheric_pressure REAL,
                    cloud_cover REAL,
                );
            '''

            self.cursor.executescript(create_table_query)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise DatabaseError(e)

    def commit(self):
        try:
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise DatabaseError(e)

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            raise DatabaseError(e)
