import threading
import time
import server
import file_generation
import parse_and_send

def server_thread():
    # Start the server to receive and process data
    server.main()

def processing_thread():
    # Wait for 4 hours before starting the processing
    time.sleep(4 * 60 * 60)

    db_file = "data/data.db"
    station_id = "test_station"

    while True:
        # Generate the input files for METRo model
        file_generation.generate_rwis_configuration_xml(db_file, station_id, "data/configuration.xml")
        file_generation.generate_rwis_observation_xml(db_file, station_id, "data/observation.xml")
        file_generation.generate_input_forecast_xml(db_file, station_id, "data/forecast.xml")

        # Run the METRo model
        file_generation.run_metro()

        # Parse the output of METRo model and send it
        parse_and_send.parse_and_send('roadcast.xml', '127.0.0.1', 12345)

        # Wait for 30 minutes before the next processing
        time.sleep(30 * 60)

def main():
    # Create and start the server thread
    t1 = threading.Thread(target=server_thread)
    t1.start()

    # Create and start the processing thread
    t2 = threading.Thread(target=processing_thread)
    t2.start()

if __name__ == "__main__":
    main()
