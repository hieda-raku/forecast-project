from asyncore import loop
import tornado.ioloop
import tornado.iostream
import tornado.tcpserver
from crc_checksum import CRC16
from data_process import process_json_data, process_umb_data
from database import AsyncDatabaseManager  # 导入数据库管理器


# 定义CRC效验函数
def validate_crc(data):
    """验证数据的CRC效验和。

    参数:
        data (bytes): 需要验证的数据。

    返回:
        bool: 如果CRC效验成功返回True，否则返回False。
    """
    # 提取CRC效验和
    received_crc = data[-3:-1]

    # 计算数据的CRC效验和
    calculated_crc = CRC16.calc_crc16(data[:-3]).to_bytes(2, byteorder="little")

    # 比较计算出的效验和与接收到的效验和
    if received_crc == calculated_crc:
        return True
    else:
        return False


class TCPServer(tornado.tcpserver.TCPServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_ids = {}
        # 初始化数据库管理器
        self.db_manager = AsyncDatabaseManager(
            user="hiedaraku",
            password="Henry970226",
            db="weather_station_db",
            host="localhost",
            port=3306,
        )
        # 使用Tornado的事件循环连接到数据库
        tornado.ioloop.IOLoop.current().add_callback(self.db_manager.connect)

    async def handle_stream(self, stream, address):
        """处理来自传感器站的连接和数据。

        参数:
            stream (IOStream): 与客户端的连接流。
            address (tuple): 客户端的地址。
        """
        # 读取128字节的注册包
        registration_packet = await stream.read_bytes(128, partial=True)
        # 解析注册包内容
        station_id = registration_packet[:6].decode("utf-8")
        protocol_type = registration_packet[6:].decode("utf-8").strip()  # 去除可能的额外空白字符

        # 拆分站点ID为站点类型和站点编号
        station_type = station_id[:2]
        station_number = station_id[2:]

        self.connection_ids[address] = {
            "station_id": station_id,
            "station_type": station_type,  # 新增
            "station_number": station_number,  # 新增
            "protocol_type": protocol_type,
        }

        print(f"连接来自 {address}, Station ID: {station_id}, Protocol: {protocol_type}")

        try:
            while True:
                data = await stream.read_bytes(1024, partial=True)
                if not data:
                    break

                # 根据数据类型处理数据
                if protocol_type == "JSON":
                    processed_data = process_json_data(data)
                    # 插入数据到数据库
                    await self.db_manager.ensure_table_and_insert(
                        station_type, processed_data, station_number
                    )
                elif protocol_type == "UMB":
                    # 处理UMB协议数据
                    if validate_crc(data):
                        umb_data = " ".join([f"{byte:02X}" for byte in data])
                        device_id, processed_data = process_umb_data(umb_data)
                        await self.db_manager.ensure_table_and_insert(
                            station_type + str(device_id),
                            processed_data,
                            station_number,
                        )

        except tornado.iostream.StreamClosedError:
            print(
                f"Connection from {self.connection_ids[address]['station_id']} closed"
            )
            del self.connection_ids[address]  # 从字典中删除该连接的信息
        except Exception as e:
            print(
                f"Error handling connection from {self.connection_ids[address]['station_id']}: {e}"
            )
            stream.close()

def run_server():
    server = TCPServer()
    server.listen(18120)
    print("TCP server started on port 18120")
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    run_server()