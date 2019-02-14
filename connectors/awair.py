import functools
import json
import datetime
import logging


class AwairConnector:

    def __init__(self, awairClient):
        self.client = awairClient

    @functools.lru_cache()
    def get_devices(self):
        """
        Fetch a list of Awair devices from the GraphQL server, cache results
        so we don't waste our API quota
        """

        query = """
        {
            Devices {
                devices {
                    deviceId, uuid, name
                }
            }
        }
        """

        # Run the query and parse the JSON
        result = self.run_query(query)

        try:
            return result["data"]["Devices"]["devices"]
        except KeyError as e:
            # If the format isn't as expected
            raise AwairException("Server returned data in an invalid format", result) from e

    def get_sensor_readings(self, device, after_time=None):

        if after_time is not None:
            # The API is using a greater than or equal comparison but we need just
            # greater than comparison, so add one second as a hack
            after_time += datetime.timedelta(seconds=1)

        else:
            # How far back to gather historical data the first time
            # TODO: should be configurable
            after_time = datetime.datetime.now() - datetime.timedelta(days=30)

        # Convert to an ISO format
        after_time = after_time.isoformat()

        query = """
        query getData($uuid: String!, $after: String) {
            AirData15Minute (deviceUUID: $uuid, from: $after) {
                airDataSeq {
                    timestamp,
                    score,
                    sensors {
                        component,
                        value
                    }
                }
            }
        }
        """

        results = self.run_query(query, {
            "uuid": device["uuid"],
            "after": after_time
        })

        try:
            samples = results["data"]["AirData15Minute"]["airDataSeq"]

            logging.info("   Found %s data points after %s", len(samples), after_time)

            return samples

        except KeyError as e:
            # If the format isn't as expected
            raise AwairException("Server returned data in an invalid format", result) from e

    def run_query(self, query, variables=None):
        """Query the GraphQL server and parse the results as JSON"""

        result = self.client.execute(query, variables)

        return json.loads(result)


class AwairException(Exception):
    pass
