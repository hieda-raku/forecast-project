import datetime
import socket
import json


def send_data(host, port, station_id, production_date, predictions, protocol="tcp"):
    # 确保production_date是字符串格式
    production_date_str = production_date.isoformat() if isinstance(production_date, datetime.datetime) else production_date

    # 构建包含station_id和production_date的数据字典
    data_to_send = {
        "station_id": station_id,
        "production_date": production_date_str,  # 使用字符串格式的日期
        "predictions": predictions
    }
    # 将数据序列化为JSON字符串
    json_data = json.dumps(data_to_send).encode('utf-8')
    print("datasented")

    # 创建socket对象
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM if protocol == "tcp" else socket.SOCK_DGRAM) as sock:
        if protocol == "tcp":
            sock.connect((host, port))
            sock.sendall(json_data)
        else:  # UDP does not need to establish a connection
            sock.sendto(json_data, (host, port))
