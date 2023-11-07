from multiprocessing import Process

import tornado
import server  # 假设你的server.py文件就是这个名字

def run_server():
    # 这里需要创建并连接数据库管理器实例
    db_manager = server.AsyncDatabaseManager(
        user="hiedaraku",
        password="Henry970226",
        db="weather_station_db",
        host="localhost",
        port=3306,
    )
    # 连接数据库
    tornado.ioloop.IOLoop.current().run_sync(db_manager.connect)
    # 启动服务器
    server.start_server(db_manager)

if __name__ == "__main__":
    # 创建服务器进程
    server_process = Process(target=run_server)
    server_process.start()

    # ... 这里可以创建其他进程

    # 等待服务器进程结束
    server_process.join()
