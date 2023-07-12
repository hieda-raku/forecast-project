import json
import socket
import xml.etree.ElementTree as ET

def parse_and_send(xml_file, ip, port):
    # 解析XML文件
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # 创建一个字典来保存数据
    data = {}

    # 遍历XML文件的元素并将其添加到字典中
    for child in root:
        if child.tag == 'header':
            data['header'] = {grandchild.tag: grandchild.text for grandchild in child}
        elif child.tag == 'prediction-list':
            data['prediction-list'] = [{grandchild.tag: grandchild.text for grandchild in child} for child in child]

    # 将字典转换为JSON字符串
    json_data = json.dumps(data)

    # 创建一个socket对象
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 连接到指定的IP地址和端口
    s.connect((ip, port))

    # 发送数据
    s.sendall(json_data.encode('utf-8'))

    # 关闭连接
    s.close()

# 测试代码
parse_and_send('roadcast.xml', '127.0.0.1', 12345)
