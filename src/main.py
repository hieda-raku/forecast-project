import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from client import send_data
from data_process import convert_datetime_to_string, parse_roadcast_xml
from database import AsyncDatabaseManager
from forecast_request import WeatherForecast
from server import run_server
from file_generation import MetroXMLGenerator  # 确保路径正确
from datetime import datetime, timedelta

# 配置参数
STATION_ID = "RL001"
TCP_HOST = "5831f3182v.imdo.co"
TCP_PORT = 39826
UDP_HOST = "118.195.238.196"
UDP_PORT = 28090

# 创建数据库管理器实例
db_manager = AsyncDatabaseManager(
    user="hiedaraku",
    password="Henry970226",
    db="weather_station_db",
    host="localhost",
    port=3306,
)

async def request_forecast():
    # 创建weather_forecasts表（如果还没有创建的话）
    await db_manager.create_weather_forecast_table()

    # 使用 db_manager 实例化 WeatherForecast
    weather_forecast = WeatherForecast(db_manager)
    forecasts = await weather_forecast.get_forecasts()
    await weather_forecast.store_forecasts(forecasts)

async def generate_files():
    # 创建MetroXMLGenerator实例
    xml_generator = MetroXMLGenerator(db_manager)

    # 生成指定站点的XML文件
    await xml_generator.generate_station_xml(STATION_ID)
    district_code = "101120712"
    await xml_generator.generate_forecast_xml(STATION_ID, district_code)
    station_type = "RL9"
    station_type_2 = "RL7"
    await xml_generator.generate_observation_xml(station_type, station_type_2)

async def run_model_prediction(station_id):
    command = [
        "python3",
        "/root/workspace/metro/usr/bin/metro",
        "--input-forecast",
        f"/root/workspace/forecast-project/data/{station_id}_forecast.xml",
        "--input-station",
        f"/root/workspace/forecast-project/data/{station_id}_configuration.xml",
        "--input-observation",
        f"/root/workspace/forecast-project/data/{station_id}_observation.xml",
        "--output-roadcast",
        f"/root/workspace/forecast-project/data/{station_id}_roadcast.xml"
    ]
    try:
        # 使用异步的方式运行命令，以便不阻塞事件循环
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            print("Metro命令执行成功！")
            print(stdout.decode())  # 打印命令的输出
        else:
            print(f"Metro命令执行失败，返回代码：{process.returncode}")
            print(stderr.decode())  # 打印命令的错误输出
    except Exception as e:
        print(f"执行过程中出现异常：{e}")

async def send_forecast_data(station_id):
    # 解析生成的roadcast XML文件
    production_date, predictions = parse_roadcast_xml(station_id)
    # 转换datetime对象为字符串
    converted_predictions = convert_datetime_to_string(predictions)
    # 发送转换后的数据
    send_data(TCP_HOST, TCP_PORT, station_id, production_date.isoformat(), converted_predictions, protocol="tcp")
    send_data(UDP_HOST, UDP_PORT, station_id, production_date.isoformat(), converted_predictions, protocol="udp")
    # 将解析的数据插入到数据库中
    for prediction_data in predictions:
        await db_manager.insert_roadcast_prediction(production_date, prediction_data)

async def main():
    # 连接数据库
    await db_manager.connect()

    # 启动服务器
    run_server()

    # 创建调度器实例并添加定时任务
    scheduler = AsyncIOScheduler()
    scheduler.add_job(request_forecast, 'interval', hours=1)
    scheduler.add_job(generate_files, 'interval', hours=1, next_run_time=datetime.now() + timedelta(seconds=10))
    scheduler.add_job(run_model_prediction, 'interval', hours=1, args=[STATION_ID], next_run_time=datetime.now() + timedelta(seconds=20))
    scheduler.add_job(send_forecast_data, 'interval', hours=1, args=[STATION_ID], next_run_time=datetime.now() + timedelta(seconds=30))
    scheduler.start()

    try:
        # 运行直到被取消或者程序结束
        await asyncio.Future()
    finally:
        # 关闭数据库连接
        await db_manager.close()
        scheduler.shutdown()
        print("程序执行完毕！")

if __name__ == "__main__":
    asyncio.run(main())