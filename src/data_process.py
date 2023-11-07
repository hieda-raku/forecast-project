import json
import struct
import time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

road_condition_mapping = {
    10: 33,  # 干
    15: 34,  # 潮
    20: 34,  # 湿
    25: 34,  # 潮
    30: 34,  # 湿
    35: 35,  # 冰
    40: 35,  # 雪
    45: 40,  # 霜
}

channel_to_field = {
    7: {
        100: "air_temperature",
        110: "dewpoint_temperature",
        200: "humidity",
        300: "atmospheric_pressure",
        401: "wind_speed",
        501: "wind_direction",
        620: "absolute_precipitation",
        625: "realitive_precipitation",
        820: "rainfall_intensity",
    },
    9: {
        101: "road_surface_temperature",
        151: "freezing_point",
        601: "water_film_height",
        801: "saline_concent",
        810: "ice_percentage",
        820: "friction",
        900: "road_condition",
    },
}


def handle_simple_data(byte_list, idx, device_id, channel_dex):
    """处理简单数据类型（如路面状况）并返回解析后的数据和下一个索引位置"""
    field_name = channel_to_field[device_id].get(channel_dex)
    value_dex = road_condition_mapping[int(byte_list[idx], 16)]
    return {field_name: value_dex}, idx + 1


def handle_float_data(byte_list, idx, device_id, channel_dex):
    """处理浮点数据类型并返回解析后的数据和下一个索引位置"""
    value_hex = "".join(byte_list[idx : idx + 4][::-1])
    field_name = channel_to_field[device_id].get(channel_dex)
    value_dex = round(struct.unpack("!f", bytes.fromhex(value_hex))[0], 2)
    return {field_name: value_dex}, idx + 4


def extract_data_from_raw(byte_list, idx, device_id, channel_dex, data_type):
    """根据数据类型提取数据，并返回解析后的数据和下一个索引位置"""
    if data_type in ["10", "11"]:
        return handle_simple_data(byte_list, idx, device_id, channel_dex)
    else:
        return handle_float_data(byte_list, idx, device_id, channel_dex)


def get_channel_info(byte_list, idx):
    """获取通道的基本信息：错误码、通道索引和数据类型，并返回这些信息及下一个索引位置"""
    error_code = byte_list[idx]
    idx += 1

    channel_dex = int("".join(byte_list[idx : idx + 2][::-1]), 16)
    idx += 2

    data_type = byte_list[idx]
    idx += 1

    return error_code, channel_dex, data_type, idx


def parse_single_channel(byte_list, idx, device_id):
    """解析单个通道的数据，并返回解析后的数据和下一个索引位置"""
    data_len = int(byte_list[idx], 16)
    idx += 1

    error_code, channel_dex, data_type, idx = get_channel_info(byte_list, idx)

    if error_code == "00":
        parsed_data, idx = extract_data_from_raw(
            byte_list, idx, device_id, channel_dex, data_type
        )
        return parsed_data, idx
    else:
        error_code_int = int(error_code, 16)
        field_name = channel_to_field[device_id].get(
            channel_dex, f"UnknownField_{channel_dex}"
        )
        # 跳过错误数据的剩余部分
        idx += data_len - 4
        return {field_name: f"errorcode{error_code_int}"}, idx


def parse_umb_data(byte_list):
    """解析整个UMB数据包，并返回一个包含所有通道数据的列表"""
    data_list = []
    idx = 12
    device_id = int(byte_list[5][0], 16)
    channel_number = int(byte_list[11], 16)

    # 获取当前的时间戳（毫秒级）
    timestamp = int(time.time() * 1000)

    # 在data_list的前面添加时间戳
    data_list.append({"timestamp": timestamp})

    for _ in range(channel_number):
        parsed_data, idx = parse_single_channel(byte_list, idx, device_id)
        data_list.append(parsed_data)

    return device_id, data_list


def process_umb_data(data):
    """处理UMB数据，并打印解析后的结果"""
    byte_list = data.split()
    device_id, parsed_data = parse_umb_data(byte_list)
    return device_id, parsed_data


def process_json_data(data):
    # 将字节数据解码为字符串
    data_str = data.decode("utf-8")

    # 使用json模块解析字符串数据
    json_data = json.loads(data_str)

    # 提取时间戳
    timestamp = json_data.get("ts")

    # 提取params字段下的数据
    params_data = json_data.get("params", {})

    # 将params数据转换为与UMB格式相同的列表格式
    parsed_data = [{"timestamp": timestamp}] + [
        {key: value} for key, value in params_data.items()
    ]

    return parsed_data

def parse_roadcast_xml(station_id):
     # 根据station_id构建文件路径
    file_path = f"/root/workspace/forecast-project/data/{station_id}_roadcast.xml"
    
    # 读取并解析XML文件
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    production_date = root.find("header/production-date").text
    # 将UTC时间转换为+8时区
    production_date = production_date.replace("Z", "+00:00")
    production_date = datetime.fromisoformat(production_date) + timedelta(hours=8)

    
    predictions = []
    for prediction_elem in root.findall("prediction-list/prediction"):
        
        roadcast_time = prediction_elem.find("roadcast-time").text.replace("Z", "+00:00")
        roadcast_time = datetime.fromisoformat(roadcast_time) + timedelta(hours=8)
        
        prediction_data = {
            "station_id": station_id,
            "production_date": production_date,
            "roadcast_time": roadcast_time,
            "time_distance": float(prediction_elem.find("hh").text),
            "air_tempreture": float(prediction_elem.find("at").text),
            "dew_point":float(prediction_elem.find("td").text),
            "wind_speed":float(prediction_elem.find("ws").text),
            "snow_quantity":float(prediction_elem.find("sn").text),
            "rain_quantity":float(prediction_elem.find("ra").text),
            "total_snow_preci":float(prediction_elem.find("qp-sn").text),
            "total_rain_preci":float(prediction_elem.find("qp-ra").text),
            "cloud_coverage":float(prediction_elem.find("cc").text),
            "solar_flux":float(prediction_elem.find("sf").text),
            "infra_red_flux":float(prediction_elem.find("ir").text),
            "vapor_flux":float(prediction_elem.find("fv").text),
            "sensible_heat":float(prediction_elem.find("fc").text),
            "anthropogenic_flux":float(prediction_elem.find("fa").text),
            "ground_exchange_flux":float(prediction_elem.find("fg").text),
            "blackbody":float(prediction_elem.find("bb").text),
            "phase_change":float(prediction_elem.find("fp").text),
            "road_condition":float(prediction_elem.find("rc").text),
            "surface_tempreature":float(prediction_elem.find("st").text),
            "sub_surface_tempreature": float(prediction_elem.find("sst").text)
        }
        predictions.append(prediction_data)

    return production_date, predictions

def convert_datetime_to_string(predictions):
    new_predictions = []
    for prediction in predictions:
        new_prediction = prediction.copy()
        new_prediction['production_date'] = new_prediction['production_date'].isoformat()
        new_prediction['roadcast_time'] = new_prediction['roadcast_time'].isoformat()
        new_predictions.append(new_prediction)
    return new_predictions
