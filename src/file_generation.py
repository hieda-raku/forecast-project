import datetime
import math
import xml.etree.ElementTree as ET
from database import AsyncDatabaseManager, DataAggregator


def calculate_dew_point(temp, rh):
    a = 17.27
    b = 237.7
    alpha = math.log(rh / 100.0) + (a * temp) / (b + temp)
    dew_point = (b * alpha) / (a - alpha)
    return round(dew_point, 2)


def check_data_continuity(forecast_data):
    continuous_data = []
    for i in range(len(forecast_data) - 1):
        current_data_time = forecast_data[i][10]  # 假设data_time是第10个字段
        next_data_time = forecast_data[i + 1][10]
        time_difference = next_data_time - current_data_time
        if time_difference.total_seconds() > 3600:  # 如果时间差超过1小时
            break
        continuous_data.append(forecast_data[i])
        # 添加最后一个数据点
    if forecast_data:
        continuous_data.append(forecast_data[-1])
    return continuous_data


class MetroXMLGenerator:
    def __init__(self, db_manager):
        self.db_manager = db_manager  # 初始化数据库管理器
        self.aggregator = DataAggregator(db_manager)

    async def generate_observation_xml(
        self, road_condition_station_id, meteorology_station_id=None
    ):
        # 调整生成的观测数据的时间
        hours = 6

        if meteorology_station_id:
            aggregated_data = await self.aggregator.aggregate_data(
                hours, road_condition_station_id, meteorology_station_id
            )
        else:
            aggregated_data = await self.aggregator.aggregate_data(
                hours, road_condition_station_id
            )

        # 创建观测的XML元素
        root = self._create_observation_element(aggregated_data)

        if road_condition_station_id == 'RL9':
            self._save_to_file(root, f"RL001_observation")
        else:
            self._save_to_file(root, f"{road_condition_station_id}_observation")

    def _create_observation_element(self, observation_data):
        # 创建observation根元素
        root = ET.Element("observation")
        # 添加header子元素
        header_elem = ET.SubElement(root, "header")
        # 设置header的子元素属性
        ET.SubElement(header_elem, "filetype").text = "rwis-observation"
        ET.SubElement(header_elem, "version").text = "1.0"
        ET.SubElement(
            header_elem, "road-station"
        ).text = "third-party-observation"  # 根据需要更改
        
        # 处理production-date
        current_time = datetime.datetime.now() - datetime.timedelta(hours=8)
        ET.SubElement(
            header_elem, "production-date"
        ).text = current_time.strftime("%Y-%m-%dT%H:%M") + "Z"

        # 创建每个观测数据的列表
        measure_list_elem = ET.SubElement(root, "measure-list")
        for data in observation_data:
            measure_elem = ET.SubElement(measure_list_elem, "measure")
            observation_time = ET.SubElement(measure_elem, "observation-time")
            
            # 转换时间戳为datetime对象，然后减去8小时
            adjusted_time = datetime.datetime.fromtimestamp(data.pop("timestamp") / 1000) - datetime.timedelta(hours=8)
            
            # 格式化时间，只保留到分钟，并转换为UTC格式
            observation_time.text = adjusted_time.strftime("%Y-%m-%dT%H:%M") + "Z"

            if "pi" in data:
                ET.SubElement(measure_elem, "pi").text = "1" if data["pi"] > 0 else "0"
                data.pop('pi')

            for key, value in data.items():
                ET.SubElement(measure_elem, key).text = str(value)
            
            ET.SubElement(measure_elem, "sst").text = str(data["st"])

        return root



    async def generate_forecast_xml(self, station_id, district_code):
        # 从数据库中获取气象预报数据
        forecast_data = await self.db_manager.get_weather_forecasts(district_code)
        # 检查数据的连续性
        continuous_data = check_data_continuity(forecast_data)
        # 创建预测的XML元素
        root = self._create_forecast_element(continuous_data)
        # 保存XML到文件
        self._save_to_file(root, f"{station_id}_forecast")

    def _create_forecast_element(self, forecast_data):
        # 创建forecast根元素
        root = ET.Element("forecast")
        # 添加header子元素
        header_elem = ET.SubElement(root, "header")
        # 设置header的子元素属性
        ET.SubElement(header_elem, "filetype").text = "forecast"
        ET.SubElement(header_elem, "version").text = "1.1"
        ET.SubElement(header_elem, "station-id").text = "third-party-forecast"
        
        # 处理production-date
        current_time = datetime.datetime.now() - datetime.timedelta(hours=8)
        ET.SubElement(
            header_elem, "production-date"
        ).text = current_time.strftime("%Y-%m-%dT%H:%M") + "Z"

        # 创建每小时的预测列表
        hourly_fcst_list_elem = ET.SubElement(root, "prediction-list")
        for data in forecast_data:
            hourly_fcst_elem = ET.SubElement(hourly_fcst_list_elem, "prediction")

            # 判断sn的值
            hour_of_day = data[10].hour
            if 13 <= hour_of_day <= 17:
                sn_value = str(data[7])  # 使用降雨量
            else:
                sn_value = "0"

            # 将cc从百分比转换为8分量
            cc_value = str(int(data[9] * 8 / 100))
            # 处理forecast-time
            forecast_time = data[10] - datetime.timedelta(hours=8)
            elements = [
                ("forecast-time", forecast_time.strftime("%Y-%m-%dT%H:%M") + "Z"),
                ("at", str(data[3])),
                ("td", str(calculate_dew_point(data[3], data[6]))),
                ("ws", str(data[4])),
                ("sn", sn_value),
                ("ra", str(data[7])),
                ("ap", str(data[8])),
                ("cc", cc_value),
            ]
            for name, text in elements:
                ET.SubElement(hourly_fcst_elem, name).text = text

        return root


    async def generate_station_xml(self, station_id):
        # 从数据库中获取站点和路面层数据
        station_data = await self.db_manager.get_station_data(station_id)
        roadlayer_data = await self.db_manager.get_roadlayer_data(station_id)
        # 创建站点的XML元素
        root = self._create_station_element(station_data, roadlayer_data)
        # 保存XML到文件
        self._save_to_file(root, f"{station_id}_configuration")

    def _create_station_element(self, station_data, roadlayer_data):
        # 创建站点的根元素
        root = ET.Element("station")
        # 添加header和路面层列表子元素
        root.append(self._create_header_element(station_data))
        root.append(self._create_roadlayer_list_element(roadlayer_data))
        return root

    def _create_header_element(self, station_data):
        # 创建header元素并设置其属性
        header = ET.Element("header")
        elements = [
            ("filetype", "rwis-configuration"),
            ("version", "1.0"),
            ("road-station", station_data["station_id"]),
            ("production-date", station_data["production_date"].isoformat()),
        ]
        for name, text in elements:
            ET.SubElement(header, name).text = text

        # 设置坐标子元素
        coordinate = ET.SubElement(header, "coordinate")
        ET.SubElement(coordinate, "latitude").text = str(station_data["latitude"])
        ET.SubElement(coordinate, "longitude").text = str(station_data["longitude"])
        ET.SubElement(header, "station-type").text = station_data["station_type"]
        return header

    def _create_roadlayer_list_element(self, roadlayer_data):
        # 创建路面层列表元素
        roadlayer_list = ET.Element("roadlayer-list")
        for layer in roadlayer_data:
            roadlayer = ET.SubElement(roadlayer_list, "roadlayer")
            elements = [
                ("position", str(layer["position"])),
                ("type", layer["type"]),
                ("thickness", str(layer["thickness"])),
            ]
            for name, text in elements:
                ET.SubElement(roadlayer, name).text = text
        return roadlayer_list

    def _save_to_file(self, root, filename):
        # 将XML内容保存到文件
        tree = ET.ElementTree(root)
        file_path = f"../data/{filename}.xml"
        with open(file_path, "wb") as file:
            tree.write(file)

import asyncio


async def test_generate_station_xml():
    # 创建数据库管理器实例并连接
    db_manager = AsyncDatabaseManager(
        user="hiedaraku",
        password="Henry970226",
        db="weather_station_db",
        host="localhost",
        port=3306,
    )
    await db_manager.connect()

    # 创建MetroXMLGenerator实例
    xml_generator = MetroXMLGenerator(db_manager)

    # 生成指定站点的XML文件
    station_id = "RL001"  # 你可以替换为你想要的站点ID
    await xml_generator.generate_station_xml(station_id)
    district_code = "101120712"
    await xml_generator.generate_forecast_xml(station_id, district_code)
    station_type = "RL9"
    station_type_2 = "RL7"
    await xml_generator.generate_observation_xml(station_type, station_type_2)

    # 关闭数据库连接
    await db_manager.close()


# 运行测试函数
asyncio.run(test_generate_station_xml())
