import socket
from xml.etree import ElementTree as ET

def read_xml_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def tcp_client(file_path,ip,port):
    
    # 获取服务器地址和端口号
    server_ip = ip
    server_port = port
    # 从本地文件读取XML数据
    xml_data = read_xml_file(file_path)

    try:
        # 创建TCP套接字
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 连接到服务器
        client_socket.connect((server_ip, server_port))

        # 发送XML数据到服务器
        client_socket.send(xml_data.encode('utf-8'))

        #确认发送
        print('send success')

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 关闭连接
        client_socket.close()
