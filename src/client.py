import socket
from xml.etree import ElementTree as ET

def read_xml_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def tcp_client(file_path):
    # 读取XML配置文件以获取服务器配置信息
    tree = ET.parse('src/config.xml')
    root = tree.getroot()

    # 获取服务器地址和端口号
    server_ip = root.find('clinet_host').text
    server_port = int(root.find('clinet_port').text)
    # 从本地文件读取XML数据
    xml_data = read_xml_file(file_path)
    
    try:
        # 创建TCP套接字
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # 发送XML数据到服务器
        client_socket.sendto(xml_data.encode('utf-8'),(server_ip,server_port))

        #确认发送
        print('send success')
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 关闭连接
        client_socket.close()

# XML文件的本地路径
file_path = 'data/RLfreeway_roadcast.xml'

# 启动客户端
tcp_client(file_path)