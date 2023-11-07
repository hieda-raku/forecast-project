import subprocess


def run_metro(station_id):
    command = [
        "python3",
        "/root/workspace/metro/usr/bin/metro",
        "--input-forecast",
        f"/root/workspace/forecast-project/data/{station_id}_forecast.xml",
        "--input-station",
        f"/root/workspace/forecast-project/data/{station_id}_configuration.xml",
        "--input-observation",
        f"/root/workspace/forecast-project/data/{station_id}_observation.xml",
        "--output-roadcast",
        f"/root/workspace/forecast-project/data/{station_id}_roadcast.xml"
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        print(result.stdout)  # 打印命令的输出
        if result.returncode == 0:
            print("Metro命令执行成功！")
        else:
            print(f"Metro命令执行失败，返回代码：{result.returncode}")
            print(result.stderr)  # 打印命令的错误输出
    except Exception as e:
        print(f"执行过程中出现异常：{e}")
