import threading
import time
import server
import file_generation
from client import parse_and_send
 # 创建一个线程锁对象
db_lock = threading.Lock()
def server_thread():
    # 启动服务器以接收和处理数据
    server.server(db_lock)
def processing_thread():
    # 等待4小时后开始处理
    time.sleep(4 * 60 * 60)
    db_file = "data/data.db"
    station_id = "test_station"
    while True:
        with db_lock:
            # 生成METRo模型的输入文件
            file_generation.generate_rwis_configuration_xml(db_file, station_id, "data/configuration.xml")
            file_generation.generate_rwis_observation_xml(db_file, station_id, "data/observation.xml")
            file_generation.generate_input_forecast_xml(db_file, station_id, "data/forecast.xml")
             # 运行METRo模型
            file_generation.run_metro()
             # 解析METRo模型的输出并发送
            parse_and_send('roadcast.xml', '127.0.0.1', 12345)
             # 在下一次处理之前等待30分钟
            time.sleep(30 * 60)
def main():
    # 创建并启动服务器线程
    t1 = threading.Thread(target=server_thread)
    t1.start()
     # 创建并启动处理线程
    t2 = threading.Thread(target=processing_thread)
    t2.start()
if __name__ == "__main__":
    main()