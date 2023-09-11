import threading
import time
import server
import file_generation
import forecast_request
from database import DatabaseManager
from client import tcp_client
 # 创建一个线程锁对象
db_lock = threading.Lock()
db_manager = DatabaseManager('../data/data.sqlite')
def server_thread():
    # 启动服务器以接收和处理数据
    server.server(db_lock)
def processing_thread():
    db_file = "../data/data.sqlite" 
    station_id = "RLfreeway"
    forecast_city = '任城'
    while True:
        with db_lock:
            db_manager.connect()
            forecast_request.get_forecast_data(db_manager)
            # 生成METRo模型的输入文件
            #file_generation.generate_rwis_configuration_xml(db_file, station_id, f"../data/{station_id}_configuration.xml")
            file_generation.generate_rwis_observation_xml(db_file, station_id, f"../data/{station_id}_observation.xml")
            file_generation.generate_input_forecast_xml(db_file, station_id, forecast_city,f"../data/{station_id}_forecast.xml")
        
        # 运行METRo模型
        file_generation.run_metro(station_id)
        # 解析METRo模型的输出并发送
        tcp_client(f'../data/{station_id}_roadcast.xml','3q421e129.goho.co',21822)
        tcp_client(f'../data/{station_id}_roadcast.xml','118.195.238.196',28090)
        # 在下一次处理之前等待30分钟
        time.sleep(60 * 60)

def main():
    # 创建并启动服务器线程
    t1 = threading.Thread(target=server_thread)
    t1.start()
    # 创建并启动处理线程
    t2 = threading.Thread(target=processing_thread)
    t2.start()
if __name__ == "__main__":
    main()
