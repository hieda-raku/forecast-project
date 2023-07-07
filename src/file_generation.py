import datetime
import pytz
import xml.etree.ElementTree as ET
from database import DatabaseManager, DatabaseError

def generate_input_forecast_xml(db_file, station_id, output_file):
    # 连接数据库
    db_manager = DatabaseManager(db_file)
    db_manager.connect()

    try:
        # 查询数据库获取数据
        data = db_manager.cursor.execute("SELECT * FROM data WHERE station_id = ? ORDER BY data_time ASC", (station_id,)).fetchall()

        if len(data) < 2:
            print("数据库中的数据不足，无法生成输入文件")
            return

        # 创建根元素
        root = ET.Element("forecast")

        # 创建 header 元素
        header_element = ET.SubElement(root, "header")

        # 添加 production-date 元素
        production_date_element = ET.SubElement(header_element, "production-date")
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
            prediction_element = ET.SubElement(prediction_list_element, "prediction")

            # 添加 forecast-time 元素
            forecast_time_element = ET.SubElement(prediction_element, "forecast-time")
            forecast_time_element.text = row[2]

            # 添加具体的天气元素，使用从数据库中获取的数据
            ET.SubElement(prediction_element, "at").text = str(row[3])
            ET.SubElement(prediction_element, "td").text = str(row[4])
            ET.SubElement(prediction_element, "ra").text = str(row[5])
            ET.SubElement(prediction_element, "sn").text = str(row[6])
            ET.SubElement(prediction_element, "ws").text = str(row[7])
            ET.SubElement(prediction_element, "ap").text = str(row[8])
            ET.SubElement(prediction_element, "cc").text = str(row[9])

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
        # 查询数据库获取数据
        data = db_manager.cursor.execute("SELECT * FROM data WHERE station_id = ? ORDER BY data_time ASC", (station_id,)).fetchall()

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
            observation_time_element = ET.SubElement(measure_element, "observation-time")
            observation_time_element.text = row[2]

            # 添加具体的观测元素，使用从数据库中获取的数据
            ET.SubElement(measure_element, "at").text = str(row[3])
            ET.SubElement(measure_element, "td").text = str(row[4])
            ET.SubElement(measure_element, "pi").text = str(row[12])
            ET.SubElement(measure_element, "ws").text = str(row[7])
            ET.SubElement(measure_element, "sc").text = str(row[13])
            ET.SubElement(measure_element, "st").text = str(row[14])
            ET.SubElement(measure_element, "sst").text = str(row[15])

        # 创建 XML 树并保存到文件
        xml_tree = ET.ElementTree(root)
        xml_tree.write(output_file, encoding="utf-8", xml_declaration=True)

        print(f"已生成观测文件: {output_file}")

    except DatabaseError as e:
        print(f"数据库错误: {str(e)}")

    finally:
        if db_manager and db_manager.conn:
            db_manager.close()

# 测试代码
db_file = "data/data.db"
station_id = "test_station"
output_file = "data/rwis_observation.xml"

generate_rwis_observation_xml(db_file, station_id, output_file)

# 测试代码
db_file = "data/data.db"
station_id = "test_station"
output_file = "data/input_forecast.xml"

generate_input_forecast_xml(db_file, station_id, output_file)