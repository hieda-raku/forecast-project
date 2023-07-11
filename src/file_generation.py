import datetime
import json
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

def generate_rwis_configuration_xml(db_file, station_id, output_file):
    # 连接数据库
    db_manager = DatabaseManager(db_file)
    db_manager.connect()

    try:
        # 查询数据库获取数据
        data = db_manager.cursor.execute("SELECT * FROM stations WHERE station_id = ?", (station_id,)).fetchone()

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
        production_date_element = ET.SubElement(header_element, "production-date")
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
        road_layers_json = db_manager.cursor.execute("SELECT road_layers FROM stations WHERE station_id = ?", (station_id,)).fetchone()[0]
        road_layers_data = json.loads(road_layers_json)

        # 遍历数据生成 roadlayer 元素
        for road_layer in road_layers_data:
            # 创建 roadlayer 元素
            roadlayer_element = ET.SubElement(roadlayer_list_element, "roadlayer")

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

# 测试代码
db_file = "data/data.db"
station_id = "test_station"
output_file = "data/configuration.xml"

generate_rwis_configuration_xml(db_file, station_id, output_file)

output_file = "data/observation.xml"

generate_rwis_observation_xml(db_file, station_id, output_file)

output_file = "data/forecast.xml"

generate_input_forecast_xml(db_file, station_id, output_file)