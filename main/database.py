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
                    name TEXT,
                    latitude REAL,
                    longitude REAL,
                    road_type TEXT CHECK(road_type IN ('asphalt', 'gravel', 'sand')),
                );
                CREATE TABLE IF NOT EXISTS data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT,
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
                    road_subsurface_temperature REAL,
                    FOREIGN KEY (station_id) REFERENCES stations (station_id)
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
