import requests

areacode = '101010100'
hours = '8'
key = ''
output_type = 'xml'

def get_forecast_data(areacode, hours, key,output_type):
    url = 'http://gfapi.mlogcn.com/weather/v001/hour'
    params = {
        'areacode': areacode,
        'hours': hours,
        'key': key,
        'output_type': output_type
    }
    response = requests.get(url, params=params)
    response.encoding = 'utf-8'
    data = response.text
    # 在这里添加你的数据处理代码
    print(data)

get_forecast_data(areacode,hours,key,output_type)