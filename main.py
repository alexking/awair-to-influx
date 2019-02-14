from graphqlclient import GraphQLClient
from influxdb import InfluxDBClient
from threading import Timer
import yaml
import logging
import signal
import os
import time

from connectors import AwairConnector, InfluxConnector, AwairException

# Info level logging just for testing
# TODO: set this via an arguement
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)


def main(config):
    """Accepts configuration and connects to services"""

    # Run until we get a KeyboardInterrupt
    while True:
        logging.info("Checking for new data from Awair")

        # Setup a GraphQL client to connect to Awair
        awairClient = GraphQLClient(config["awair"]["endpoint"])
        awairClient.inject_token("Bearer " + config["awair"]["token"])

        # Setup an InfluxDB client
        influxClient = InfluxDBClient(**config["influx"])

        # Setup our Awair and Influx connectors and inject the clients
        awairConnector = AwairConnector(awairClient)
        influxConnector = InfluxConnector(influxClient)

        # Find any devices
        devices = awairConnector.get_devices()
        logging.info("Found %s Awair devices", len(devices))

        # Check each device
        for device in devices:
            logging.info(" - Looking up data for device %(name)s with UUID %(uuid)s", device)

            # Find the last data we have for it, if any, in InfluxDB
            last_data_at = influxConnector.get_last_recorded_time(device)

            # Fetch sensor readings from Awair
            samples = awairConnector.get_sensor_readings(device, last_data_at)

            # Add samples to InfluxDB
            influxConnector.add_samples(device, samples)

        # TODO: we should calculate the next time there will be data
        wait_for_seconds = 15 * 60

        logging.info("Checking again in %s seconds...", wait_for_seconds)

        # Sleep until we need to do another check
        time.sleep(wait_for_seconds)


# Load and parse the configuration
try:

    with open("config.yaml") as configFile:
        config = yaml.load(configFile)

        if ("influx" not in config or "awair" not in config):
            logging.error("Invalid config.yaml file, please use config.example.yaml as a guide")
        else:
            main(config)

except FileNotFoundError as e:
    logging.error("Please add a config.yaml file")
except AwairException as e:
    logging.error("Awair server returned unexpected results", e)
except KeyboardInterrupt as e:
    logging.warn("Stopping...")
