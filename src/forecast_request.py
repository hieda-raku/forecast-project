import aiohttp

API_URL = "http://gfapi.mlogcn.com/weather/v001/hour"
API_KEY = "Hh9rZGMHGJWFAB0nSUU5NKg6PW9ZD7nJ"  # 请替换为您的API密钥


class WeatherForecast:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def get_unique_district_codes(self):
        async with self.db_manager.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT DISTINCT district_code FROM stations")
                result = await cur.fetchall()
                return [row[0] for row in result]

    async def fetch_weather_data(self, district_code):
        params = {"areacode": district_code, "hours": 24, "key": API_KEY}
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Error fetching data for district_code: {district_code}")
                    return None

    async def get_forecasts(self):
        district_codes = await self.get_unique_district_codes()
        forecasts = {}
        for district_code in district_codes:
            forecast_data = await self.fetch_weather_data(district_code)
            if forecast_data:
                forecasts[district_code] = forecast_data
        return forecasts

    async def store_forecasts(self, forecasts):
        for district_code, forecast in forecasts.items():
            if forecast["status"] == 0:  # 确保状态码为0，表示数据有效
                await self.db_manager.insert_weather_forecast(
                    str(district_code), forecast["result"]
                )