import subprocess

def run_metro():
    command = ["python3", "/home/hieda_raku/local/metro/usr/bin/metro", "--input-forecast", "/home/hieda_raku/local/workspace/forecast-project/data/forecast.xml", "--input-station", "/home/hieda_raku/local/workspace/forecast-project/data/configuration.xml", "--input-observation", "/home/hieda_raku/local/workspace/forecast-project/data/observation.xml", "--output-roadcast", "/home/hieda_raku/local/workspace/forecast-project/data/roadcast.xml"]
    try:
        subprocess.run(command)
        #subprocess.run(command, shell=True, check=True)
        print("Metro命令执行成功！")
    except subprocess.CalledProcessError as e:
        print(f"Metro命令执行失败：{e}")

run_metro()