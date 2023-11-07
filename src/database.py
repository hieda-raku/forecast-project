import asyncio
import datetime
import aiomysql
from sqlalchemy import create_engine, MetaData

SENSOR_DICTIONARIES = {
    "RL7": {
        "air_temperature": "at",
        "dewpoint_temperature": "td",
        "realitive_precipitation": "pi",
        "wind_speed": "ws",
    },
    "RL9": {"road_condition": "sc", "road_surface_temperature": "st"},
    "ZK": {
        "WS500_AT_25904210813238": "at",
        "WS500_DPT_25904210813238": "td",
        "SL3_RainfallDiff_CS23061501": "pi",
        "WS500_WS_25904210813238": "ws",
    },
    "PL": {"RoadCond2": "sc", "RoadTemp": "st"},
}

# 常量定义
MS_PER_MINUTE = 60 * 1000
MS_PER_20_MINUTES = 20 * MS_PER_MINUTE
MS_PER_HOUR = 60 * MS_PER_MINUTE


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class AsyncDatabaseManager(metaclass=SingletonMeta):
    def __init__(self, user, password, db, host="localhost", port=3306):
        if not hasattr(self, "initialized"):  # 只初始化一次
            self.user = user
            self.password = password
            self.db = db
            self.host = host
            self.port = port
            self.metadata = MetaData()
            self.engine = create_engine(
                f"mysql+aiomysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
            )
            self.existing_tables = set()
            self.pool = None  # 使用连接池
            self.lock = asyncio.Lock()
            self.initialized = True  # 设置初始化标志

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            user=self.user,
            password=self.password,
            db=self.db,
            host=self.host,
            port=self.port,
        )

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def create_table(self, table_name, data, station_id):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                columns = ["`id` INT AUTO_INCREMENT PRIMARY KEY", "`station_id` INT"]
                for item in data:
                    key = list(item.keys())[0]
                    value = item[key]
                    if key == "timestamp":
                        columns.append(f"`{key}` BIGINT")
                    elif isinstance(value, int):
                        columns.append(f"`{key}` INT")
                    elif isinstance(value, float):
                        columns.append(f"`{key}` FLOAT")
                    else:
                        columns.append(f"`{key}` VARCHAR(255)")
                create_table_query = (
                    f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)})"
                )
                await cur.execute(create_table_query)

    async def insert_data(self, table_name, data, station_id):
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            keys = ["station_id"] + [list(item.keys())[0] for item in data]
            values = [station_id] + [list(item.values())[0] for item in data]
            await cur.execute(
                f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(values))})",
                values,
            )
            await conn.commit()

    async def table_exists(self, table_name):
        """检查指定的表是否已经存在于数据库中"""
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = await cur.fetchone()
            return result is not None

    async def ensure_table_and_insert(self, table_name, data, station_id):
        async with self.lock:
            if table_name not in self.existing_tables:
                if not await self.table_exists(table_name):
                    await self.create_table(table_name, data, station_id)
                    self.existing_tables.add(table_name)
            await self.insert_data(table_name, data, station_id)

    async def create_station_table(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS `stations` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `station_id` VARCHAR(255) UNIQUE,
                    `production_date` TIMESTAMP(6),
                    `district_code` INT,
                    `station_type` VARCHAR(255),
                    `latitude` FLOAT,
                    `longitude` FLOAT
                )
                """
                await cur.execute(create_table_query)

    async def create_roadlayer_table(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS `roadlayers` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `station_id` VARCHAR(255),
                    `position` INT,
                    `type` ENUM('asphalt', 'crushed rock', 'cement', 'sand'),
                    `thickness` FLOAT,
                    FOREIGN KEY (`station_id`) REFERENCES `stations`(`station_id`)
                )
                """
                await cur.execute(create_table_query)

    async def create_weather_forecast_table(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS `weather_forecasts` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `district_code` VARCHAR(255),
                    `code` SMALLINT,
                    `temp_fc` FLOAT,
                    `wind_speed` FLOAT,
                    `wind_angle` INT,
                    `rh` INT,
                    `prec` FLOAT,
                    `pressure` INT,
                    `clouds` INT,
                    `data_time` DATETIME,
                    `last_update` DATETIME
                )
                """
                await cur.execute(create_table_query)

    async def insert_weather_forecast(self, district_code, forecast_data):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                last_update = forecast_data["last_update"]
                for data in forecast_data["hourly_fcsts"]:
                    insert_query = """
                    INSERT INTO `weather_forecasts` (`district_code`, `code`, `temp_fc`, `wind_speed`, `wind_angle`, `rh`, `prec`, `pressure`, `clouds`, `data_time`, `last_update`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    `code` = VALUES(`code`),
                    `temp_fc` = VALUES(`temp_fc`),
                    `wind_speed` = VALUES(`wind_speed`),
                    `wind_angle` = VALUES(`wind_angle`),
                    `rh` = VALUES(`rh`),
                    `prec` = VALUES(`prec`),
                    `pressure` = VALUES(`pressure`),
                    `clouds` = VALUES(`clouds`),
                    `last_update` = VALUES(`last_update`)
                    """
                    await cur.execute(
                        insert_query,
                        (
                            district_code,
                            data["code"],
                            data["temp_fc"],
                            data["wind_speed"],
                            data["wind_angle"],
                            data["rh"],
                            data["prec"],
                            data["pressure"],
                            data["clouds"],
                            data["data_time"],
                            last_update,
                        ),
                    )
                await conn.commit()

    async def insert_station(
        self,
        station_id,
        production_date,
        district_code,
        station_type,
        latitude,
        longitude,
    ):
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO `stations` (`station_id`, `production_date`, `district_code`, `station_type`, `latitude`, `longitude`) VALUES (%s, %s,%s, %s, %s, %s)",
                (
                    station_id,
                    production_date,
                    district_code,
                    station_type,
                    latitude,
                    longitude,
                ),
            )
            await conn.commit()

    async def insert_roadlayer(self, station_id, position, layer_type, thickness):
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO `roadlayers` (`station_id`, `position`, `type`, `thickness`) VALUES (%s, %s, %s, %s)",
                (station_id, position, layer_type, thickness),
            )
            await conn.commit()

    async def get_station_data(self, station_id):
        """获取指定站点的数据"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `station_id`, `latitude`, `longitude`, `station_type`,`production_date` FROM `stations` WHERE `station_id` = %s",
                    (station_id,),
                )
                result = await cur.fetchone()
                if result:
                    return {
                        "station_id": result[0],
                        "latitude": result[1],
                        "longitude": result[2],
                        "station_type": result[3],
                        "production_date": result[4],
                    }
                else:
                    return None

    async def get_roadlayer_data(self, station_id):
        """获取指定站点的路面层数据"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT `position`, `type`, `thickness` FROM `roadlayers` WHERE `station_id` = %s",
                    (station_id,),
                )
                results = await cur.fetchall()
                roadlayers = []
                for row in results:
                    roadlayers.append(
                        {"position": row[0], "type": row[1], "thickness": row[2]}
                    )
                return roadlayers

    async def get_weather_forecasts(self, district_code):
        current_hour = datetime.datetime.now().hour
        current_date = datetime.datetime.now().date()

        # Subtract 3 hours from the current time
        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
        start_time = start_time.replace(
            minute=0, second=0, microsecond=0
        )  # Reset minutes, seconds, and microseconds to 0

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                select_query = """
                SELECT * FROM `weather_forecasts`
                WHERE `district_code` = %s AND `data_time` >= %s
                ORDER BY `data_time` ASC
                """
                await cur.execute(select_query, (district_code, start_time))
                return await cur.fetchall()

    async def check_forecast_exists(self, district_code, data_time):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                check_query = """
                SELECT COUNT(*) 
                FROM `weather_forecasts` 
                WHERE `district_code` = %s AND `data_time` = %s
                """
                await cur.execute(check_query, (district_code, data_time))
                result = await cur.fetchone()
                return result[0] > 0

    async def fetch_data_from_table(self, table_name, hours):
        # 获取当前的分钟数
        current_time = datetime.datetime.now()

        # 将结束时间转换为时间戳
        end_timestamp = int(current_time.timestamp() * 1000)
        start_timestamp = end_timestamp - hours * 3600 * 1000

        # 根据SENSOR_DICTIONARIES选择需要的列
        columns = ", ".join(SENSOR_DICTIONARIES[table_name].keys())

        # 构建SQL查询
        sql_query = f"""
        SELECT {columns} , `timestamp`
        FROM `{table_name}`
        WHERE `timestamp` BETWEEN %s AND %s
        ORDER BY `timestamp` ASC
        """

        # 使用连接池执行查询
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql_query, (start_timestamp, end_timestamp))
                all_data = await cur.fetchall()

        # 数据处理：选择间隔为20分钟的数据点
        interval_milliseconds = 20 * 60 * 1000
        selected_data = []
        for target_timestamp in range(
            start_timestamp, end_timestamp, interval_milliseconds
        ):
            if not all_data:
                continue
            closest_record = min(all_data, key=lambda x: abs(x[-1] - target_timestamp))
            selected_data.append(closest_record)
            all_data = [
                record for record in all_data if record != closest_record
            ]  # 从all_data中移除已选择的数据点

        return selected_data

    async def create_roadcast_predictions_table(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS `roadcast_predictions` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `station_id` TEXT,
                    `production_date` DATETIME,
                    `roadcast_time` DATETIME,
                    `time_distance` FLOAT,
                    `air_tempreture` FLOAT,
                    `dew_point` FLOAT,
                    `wind_speed` FLOAT,
                    `snow_quantity` FLOAT,
                    `rain_quantity` FLOAT,
                    `total_snow_preci` FLOAT,
                    `total_rain_preci` FLOAT,
                    `cloud_coverage` INT,
                    `solar_flux` FLOAT,
                    `infra_red_flux` FLOAT,
                    `vapor_flux` FLOAT,
                    `sensible_heat` FLOAT,
                    `anthropogenic_flux` FLOAT,
                    `ground_exchange_flux` FLOAT,
                    `blackbody` FLOAT,
                    `phase_change` FLOAT,
                    `road_condition` INT,
                    `surface_tempreature` FLOAT,
                    `sub_surface_tempreature` FLOAT
                );

                """
                await cur.execute(create_table_query)

    async def insert_roadcast_prediction(self, production_date, prediction_data):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                insert_query = """
                INSERT INTO `roadcast_predictions` (station_id, production_date, roadcast_time, time_distance, air_tempreture, dew_point, wind_speed ,snow_quantity, rain_quantity, total_snow_preci, total_rain_preci, cloud_coverage, solar_flux, infra_red_flux, vapor_flux, sensible_heat, anthropogenic_flux, ground_exchange_flux, blackbody, phase_change, road_condition, surface_tempreature, sub_surface_tempreature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                # 将字典转换为元组
                prediction_tuple = (
                    prediction_data["station_id"],
                    production_date,
                    prediction_data["roadcast_time"],
                    prediction_data["time_distance"],
                    prediction_data["air_tempreture"],
                    prediction_data["dew_point"],
                    prediction_data["wind_speed"],
                    prediction_data["snow_quantity"],
                    prediction_data["rain_quantity"],
                    prediction_data["total_snow_preci"],
                    prediction_data["total_rain_preci"],
                    prediction_data["cloud_coverage"],
                    prediction_data["solar_flux"],
                    prediction_data["infra_red_flux"],
                    prediction_data["vapor_flux"],
                    prediction_data["sensible_heat"],
                    prediction_data["anthropogenic_flux"],
                    prediction_data["ground_exchange_flux"],
                    prediction_data["blackbody"],
                    prediction_data["phase_change"],
                    prediction_data["road_condition"],
                    prediction_data["surface_tempreature"],
                    prediction_data["sub_surface_tempreature"],
                )

                # 执行插入操作
                await cur.execute(insert_query, prediction_tuple)
                await conn.commit()


class DataAggregator:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def aggregate_data(self, hours, road_condition_table, meteorology_table=None):
        # Fetch data from the road condition table
        data2 = await self.db_manager.fetch_data_from_table(road_condition_table, hours)

        # If meteorology_table is not provided, only use road condition data
        if not meteorology_table:
            aggregated_data = []
            for row in data2:
                aggregated_row = self.combine_single_table(road_condition_table, row)
                aggregated_data.append(aggregated_row)
            return aggregated_data

        # If meteorology_table is provided, fetch data from it
        data1 = await self.db_manager.fetch_data_from_table(meteorology_table, hours)

        # Check if both tables are empty
        if not data1 and not data2:
            raise ValueError("Both meteorology and road condition data are empty!")

        # Convert and aggregate the data using SENSOR_DICTIONARIES
        aggregated_data = []

        if not data1:  # If meteorology data is empty, only use road condition data
            for row in data2:
                aggregated_row = self.combine_single_table(road_condition_table, row)
                aggregated_data.append(aggregated_row)
            return aggregated_data

        for row1 in data1:
            for row2 in data2:
                # Check if timestamps are close enough (within 1 minute)
                if abs(row1[-1] - row2[-1]) <= 60000:  # 60000 milliseconds = 1 minute
                    aggregated_row = self.combine_rows(
                        meteorology_table, road_condition_table, row1, row2
                    )
                    aggregated_data.append(aggregated_row)
                    data2.remove(row2)  # Remove the matched row from data2
                    break

        return aggregated_data

    def combine_rows(self, meteorology_table, road_condition_table, row1, row2):
        combined_data = {
            "timestamp": row1[-1]
        }  # Assuming the timestamp is at the last index
        for index, (key, value) in enumerate(
            SENSOR_DICTIONARIES[meteorology_table].items()
        ):
            combined_data[value] = row1[index]
        for index, (key, value) in enumerate(
            SENSOR_DICTIONARIES[road_condition_table].items()
        ):
            combined_data[value] = row2[index]
        return combined_data

    def combine_single_table(self, table, row):
        combined_data = {
            "timestamp": row[-1]
        }  # Assuming the timestamp is at the last index
        for index, (key, value) in enumerate(SENSOR_DICTIONARIES[table].items()):
            combined_data[value] = row[index]
        return combined_data

    @staticmethod
    def round_to_nearest_minute(timestamp):
        # Convert the timestamp to a datetime object
        dt = datetime.datetime.fromtimestamp(
            timestamp / 1000
        )  # Assuming the timestamp is in milliseconds
        # Round to the nearest minute
        rounded_dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)
        # Convert the datetime object back to a timestamp (in milliseconds)
        rounded_timestamp = int(rounded_dt.timestamp() * 1000)
        return rounded_timestamp


# 使用单例模式的数据库管理器
db_manager = AsyncDatabaseManager(
    user="hiedaraku",
    password="Henry970226",
    db="weather_station_db",
    host="localhost",
    port=3306,
)

# 这里无论调用多少次，返回的都是同一个实例
another_db_manager = AsyncDatabaseManager(
    user="hiedaraku",
    password="Henry970226",
    db="weather_station_db",
    host="localhost",
    port=3306,
)

assert db_manager is another_db_manager  # 这将证明两个变量是同一个实例
