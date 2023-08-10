import requests
import time
from datetime import datetime
from pytz import timezone
from database import DatabaseManager
from xml.etree import ElementTree as ET

# 创建数据库管理器
db_manager = DatabaseManager('./data/data.sqlite')

#连接数据库
db_manager.connect()

beijing_tz = timezone('Asia/Shanghai')

def get_forecast_data(db_manager):
    # 读取XML配置文件
    tree = ET.parse('./src/config.xml')
    root = tree.getroot()
    
    # 获取预报地代码，预报时长，秘钥和输出类型
    areacode = root.find('areacode').text
    hours = root.find('hours').text
    key = root.find('key').text
    output_type = root.find('output_type').text

    url = 'http://gfapi.mlogcn.com/weather/v001/hour'
    params = {
        'areacode': areacode,
        'hours': hours,
        'key': key,
        'output_type': output_type
    }
    response = requests.get(url, params=params)
    response.encoding = 'utf-8'
    data = response.text  
    insert_forecast_data(data,db_manager)


def insert_forecast_data(xml_data, db_manager):
    # 解析XML数据
    root = ET.fromstring(xml_data)
    # 获取更新时间
    update_time = root.find(".//last_update").text

    # 获取城市名称
    forecast_city = root.find(".//location/name").text

    # 获取每小时的预报数据
    hourly_forecasts = root.findall(".//hourly_fcst")
    for forecast in hourly_forecasts:
        # 提取所需的数据

        #处理时间数据
        given_time_str  = forecast.find("data_time").text
        given_time  = datetime.strptime(given_time_str, "%Y-%m-%d %H:%M:%S")
        given_time_with_tz = beijing_tz.localize(given_time)
        forecast_time = given_time_with_tz.strftime("%Y-%m-%dT%H:%M:%S%z")

        weather_code = forecast.find("code").text
        forecast_temperature = float(forecast.find("temp_fc").text)
        wind_speed = float(forecast.find("wind_speed").text)
        wind_direction = float(forecast.find("wind_angle").text)
        relative_humidity = float(forecast.find("rh").text)
        precipitation = float(forecast.find("prec").text)
        cloud_cover = int(forecast.find("clouds").text) * 8 // 100  # 转换为八分量
        atmospheric_pressure = float(forecast.find("pressure").text)  # 从XML中提取大气压力

        # 检查是否已经存在具有相同预报时间的记录
        existing_record = db_manager.cursor.execute(
            "SELECT * FROM forecast WHERE forecast_time = ? AND forecast_city = ?", (forecast_time, forecast_city)
        ).fetchone()

        if existing_record:
            # 如果存在，则更新记录
            update_query = '''
                UPDATE forecast SET
                    update_time = ?,
                    weather_code = ?,
                    forecast_temperature = ?,
                    wind_speed = ?,
                    wind_direction = ?,
                    relative_humidity = ?,
                    precipitation = ?,
                    atmospheric_pressure = ?,
                    cloud_cover = ?
                WHERE forecast_time = ? AND forecast_city = ?
            '''
            values = (
                update_time, weather_code,
                forecast_temperature, wind_speed, wind_direction,
                relative_humidity, precipitation, atmospheric_pressure, cloud_cover,
                forecast_time, forecast_city
            )
            db_manager.cursor.execute(update_query, values)
        else:
            # 如果不存在，则插入新记录
            insert_query = '''
                INSERT INTO forecast (
                    update_time, forecast_time, forecast_city, weather_code,
                    forecast_temperature, wind_speed, wind_direction,
                    relative_humidity, precipitation, atmospheric_pressure, cloud_cover
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            values = (
                update_time, forecast_time, forecast_city, weather_code,
                forecast_temperature, wind_speed, wind_direction,
                relative_humidity, precipitation, atmospheric_pressure, cloud_cover
            )
            db_manager.cursor.execute(insert_query, values)

    # 提交事务
    db_manager.commit()

get_forecast_data(db_manager)