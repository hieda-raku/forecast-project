import asyncio
from xml_process import parse_station_xml  # 替换为您的XML解析模块的实际名称
from database import AsyncDatabaseManager  # 替换为您的数据库模块的实际名称


async def station_process():
    stations_data = parse_station_xml("src/station_info.xml")
    db_manager = AsyncDatabaseManager(
        user="hiedaraku",
        password="Henry970226",
        db="weather_station_db",
        host="localhost",
        port=3306,
    )
    await db_manager.connect()
    await db_manager.create_station_table()
    await db_manager.create_roadlayer_table()
    # 连接到数据库
    # 遍历每个站点数据并插入到数据库中
    for station in stations_data:
        await db_manager.insert_station(
            station["station_id"],
            station["production_date"],
            station["district_code"],
            station["station_type"],
            station["latitude"],
            station["longitude"],
        )
        for layer in station["roadlayers"]:
            print(type(station), station)
            print(type(layer), layer)
            await db_manager.insert_roadlayer(
                station["station_id"],
                layer["position"],
                layer["type"],
                layer["thickness"],
            )

    # 关闭数据库连接
    await db_manager.close()


if __name__ == "__main__":
    asyncio.run(station_process())
