import datetime
import math
import json
import subprocess
import xml.etree.ElementTree as ET
from database import DatabaseManager, DatabaseError


def calculate_dew_point(temperature, humidity):
    A = 17.27
    B = 237.7
    alpha = ((A * temperature) / (B + temperature)) + math.log(humidity/100.0)
    dew_point = (B * alpha) / (A - alpha)
    return round(dew_point, 2)

def generate_input_forecast_xml(db_file, station_id, forecast_city, output_file):
    # 连接数据库
    db_manager = DatabaseManager(db_file)
    db_manager.connect()

    try:
        # 获取当前时间
        current_time = datetime.datetime.now()

        # 找到最接近当前时间的整点时间
        current_hour = current_time.replace(minute=0, second=0, microsecond=0)

        # 将整点时间格式化为与数据库中的时间格式相匹配的字符串
        start_time = current_hour.strftime("%Y-%m-%d %H:%M:%S")

        # 查询数据库获取数据
        query = """SELECT * FROM forecast 
                WHERE forecast_time >= ? AND forecast_city = ? 
                ORDER BY forecast_time ASC"""
        data = db_manager.cursor.execute(query, (start_time, forecast_city)).fetchall()


        if len(data) < 2:
            print("数据库中的数据不足，无法生成输入文件")
            return

        # 创建根元素
        root = ET.Element("forecast")

        # 创建 header 元素
        header_element = ET.SubElement(root, "header")

        # 添加 production-date 元素
        production_date_element = ET.SubElement(
            header_element, "production-date")
        production_date_element.text = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ")

        # 添加 version 元素
        version_element = ET.SubElement(header_element, "version")
        version_element.text = "1.1"

        # 添加 filetype 元素
        filetype_element = ET.SubElement(header_element, "filetype")
        filetype_element.text = "forecast"

        # 添加 station-id 元素
        station_id_element = ET.SubElement(header_element, "station-id")
        station_id_element.text = station_id

        # 创建 prediction-list 元素
        prediction_list_element = ET.SubElement(root, "prediction-list")

        # 遍历数据生成 prediction 元素
        for row in data:
            # 创建 prediction 元素
            prediction_element = ET.SubElement(
                prediction_list_element, "prediction")

            # 添加 forecast-time 元素
            forecast_time_element = ET.SubElement(
                prediction_element, "forecast-time")
            forecast_time_element.text = row[2]

            # 添加具体的天气元素，使用从数据库中获取的数据
            temperature = int(row[5])
            humidity = int(row[8])
            #空气温度
            ET.SubElement(prediction_element, "at").text = str(temperature)
            #计算露点温度
            ET.SubElement(prediction_element, "td").text = str(calculate_dew_point(temperature, humidity))
            #降雨量
            ET.SubElement(prediction_element, "ra").text = str(row[9])
            # 降雪量，需要先判定是否有降雪,然后暂时用降雨量代替
            ET.SubElement(prediction_element, "sn").text = str(row[9])
            #风速
            ET.SubElement(prediction_element, "ws").text = str(row[6])
            #大气压
            ET.SubElement(prediction_element, "ap").text = str(row[10])
            #云量
            ET.SubElement(prediction_element, "cc").text = str(int(row[11]))

        # 创建 XML 树并保存到文件
        xml_tree = ET.ElementTree(root)
        xml_tree.write(output_file, encoding="utf-8", xml_declaration=True)

        print(f"已生成输入文件: {output_file}")

    except DatabaseError as e:
        print(f"数据库错误: {str(e)}")

    finally:
        if db_manager and db_manager.conn:
            db_manager.close()
 
def generate_rwis_observation_xml(db_file, station_id, output_file):
    # 连接数据库
    db_manager = DatabaseManager(db_file)
    db_manager.connect()

    try:
        # 计算12小时之前的时间戳
        time_threshold = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        time_threshold_str = time_threshold.strftime("%Y-%m-%dT%H:%M:%SZ")

        # 查询数据库获取过去12个小时内的数据
        data = db_manager.cursor.execute(
            "SELECT * FROM observation WHERE station_id = ? AND data_time >= ? ORDER BY data_time ASC",
            (station_id, time_threshold_str)).fetchall()

        if len(data) < 1:
            print("数据库中没有相关数据，无法生成观测文件")
            return

        # 创建根元素
        root = ET.Element("observation")

        # 创建 header 元素
        header_element = ET.SubElement(root, "header")

        # 添加 filetype 元素
        filetype_element = ET.SubElement(header_element, "filetype")
        filetype_element.text = "rwis-observation"

        # 添加 version 元素
        version_element = ET.SubElement(header_element, "version")
        version_element.text = "1.0"

        # 添加 road-station 元素
        road_station_element = ET.SubElement(header_element, "road-station")
        road_station_element.text = station_id

        # 创建 measure-list 元素
        measure_list_element = ET.SubElement(root, "measure-list")

        # 遍历数据生成 measure 元素
        for row in data:
            # 创建 measure 元素
            measure_element = ET.SubElement(measure_list_element, "measure")

            # 添加 observation-time 元素
            observation_time_element = ET.SubElement(
                measure_element, "observation-time")
            observation_time_element.text = row[2]

            # 添加具体的观测元素，使用从数据库中获取的数据
            ET.SubElement(measure_element, "at").text = str(row[3])
            ET.SubElement(measure_element, "td").text = str(row[4])
            if row[10] == 0 :
                ET.SubElement(measure_element, "pi").text = '0'
            else:
                ET.SubElement(measure_element, "pi").text = '1'
            ET.SubElement(measure_element, "ws").text = str(row[7])
            ET.SubElement(measure_element, "sc").text = str(row[18])
            ET.SubElement(measure_element, "st").text = str(row[12])
            ET.SubElement(measure_element, "sst").text = str(row[12])

        # 创建 XML 树并保存到文件
        xml_tree = ET.ElementTree(root)
        xml_tree.write(output_file, encoding="utf-8", xml_declaration=True)

        print(f"已生成观测文件: {output_file}")

    except DatabaseError as e:
        print(f"数据库错误: {str(e)}")

    finally:
        if db_manager and db_manager.conn:
            db_manager.close()


def generate_rwis_configuration_xml(db_file, station_id, output_file):
    # 连接数据库
    db_manager = DatabaseManager(db_file)
    db_manager.connect()

    try:
        # 查询数据库获取数据
        data = db_manager.cursor.execute(
            "SELECT * FROM stations WHERE station_id = ?", (station_id,)).fetchone()

        if data is None:
            print("数据库中没有相关数据，无法生成配置文件")
            return

        # 创建根元素
        root = ET.Element("station")

        # 创建 header 元素
        header_element = ET.SubElement(root, "header")

        # 添加 filetype 元素
        filetype_element = ET.SubElement(header_element, "filetype")
        filetype_element.text = "rwis-configuration"

        # 添加 version 元素
        version_element = ET.SubElement(header_element, "version")
        version_element.text = "1.0"

        # 添加 road-station 元素
        road_station_element = ET.SubElement(header_element, "road-station")
        road_station_element.text = data[1]

        # 添加 production-date 元素
        production_date_element = ET.SubElement(
            header_element, "production-date")
        production_date_element.text = data[2]

        # 创建 coordinate 元素
        coordinate_element = ET.SubElement(header_element, "coordinate")

        # 添加 latitude 元素
        latitude_element = ET.SubElement(coordinate_element, "latitude")
        latitude_element.text = str(data[3])

        # 添加 longitude 元素
        longitude_element = ET.SubElement(coordinate_element, "longitude")
        longitude_element.text = str(data[4])

        # 添加 station-type 元素
        station_type_element = ET.SubElement(header_element, "station-type")
        station_type_element.text = data[5]

        # 创建 roadlayer-list 元素
        roadlayer_list_element = ET.SubElement(root, "roadlayer-list")

        # 查询数据库获取 road layers 数据
        road_layers_json = db_manager.cursor.execute(
            "SELECT road_layers FROM stations WHERE station_id = ?", (station_id,)).fetchone()[0]
        road_layers_data = json.loads(road_layers_json)

        # 遍历数据生成 roadlayer 元素
        for road_layer in road_layers_data:
            # 创建 roadlayer 元素
            roadlayer_element = ET.SubElement(
                roadlayer_list_element, "roadlayer")

            # 添加 position 元素
            position_element = ET.SubElement(roadlayer_element, "position")
            position_element.text = str(road_layer['position'])

            # 添加 type 元素
            type_element = ET.SubElement(roadlayer_element, "type")
            type_element.text = road_layer['type']

            # 添加 thickness 元素
            thickness_element = ET.SubElement(roadlayer_element, "thickness")
            thickness_element.text = str(road_layer['thickness'])

        # 创建 XML 树并保存到文件
        xml_tree = ET.ElementTree(root)
        xml_tree.write(output_file, encoding="utf-8", xml_declaration=True)

        print(f"已生成配置文件: {output_file}")

    except DatabaseError as e:
        print(f"数据库错误: {str(e)}")

    finally:
        if db_manager and db_manager.conn:
            db_manager.close()


def run_metro(station_id):
    command = ["python3", 
               "/root/workspace/metro/usr/bin/metro", 
               "--input-forecast", 
               f"/root/workspace/forecast-project/data/{station_id}forecast.xml", 
               "--input-station", 
               "/root/workspace/forecast-project/data/configuration.xml",
               "--input-observation", 
               "/root/workspace/forecast-project/data/observation.xml", 
               "--output-roadcast", 
               "/root/workspace/forecast-project/data/roadcast.xml"]
    subprocess.run(command)