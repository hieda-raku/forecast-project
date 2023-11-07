import xml.etree.ElementTree as ET


def parse_station_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    stations = []

    for station_elem in root.findall("station"):
        station_data = {}
        for child in station_elem:
            if child.tag == "coordinate":
                station_data["latitude"] = float(child.find("latitude").text)
                station_data["longitude"] = float(child.find("longitude").text)
            elif child.tag == "roadlayer_list":
                roadlayers = []
                for roadlayer_elem in child.findall("roadlayer"):
                    roadlayer_data = {}
                    for rl_child in roadlayer_elem:
                        roadlayer_data[rl_child.tag] = rl_child.text
                    roadlayers.append(roadlayer_data)
                station_data["roadlayers"] = roadlayers
            else:
                station_data[child.tag] = child.text
        stations.append(station_data)

    return stations


if __name__ == "__main__":
    stations = parse_station_xml("test/station_info.xml")
    for station in stations:
        print(station)
