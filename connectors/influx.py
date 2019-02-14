import json
import itertools
import dateutil.parser


class InfluxConnector:

    def __init__(self, influxClient):
        self.client = influxClient

    def get_last_recorded_time(self, device):
        """Returns a datetime with the last sample that we have in Influx"""

        # Find the last datapoint
        query = """
            SELECT LAST(score), time
            FROM air_quality
            WHERE host = $uuid
        """

        result = self.run_query(query, {"uuid": device["uuid"]})

        # Grab the first row
        results = list(result.get_points())

        # If we didn't get any records then return None
        if len(results) == 0:
            return None

        return dateutil.parser.parse(results[0]["time"])

    def add_samples(self, device, samples):
        """Import samples from a device into Influx"""

        records = []

        for sample in samples:

            # Convert the sensors to a dictionary with lowercase keys
            sensors = dict((i["component"].lower(), i["value"]) for i in sample["sensors"])

            # Add the score
            sensors["score"] = sample["score"]

            records.append({
                "measurement": "air_quality",
                "tags": {
                    "host": device["uuid"]
                },
                "fields": sensors,
                "time": sample["timestamp"]
            })

        self.client.write_points(records, time_precision="m")

    def run_query(self, query, params):
        """Run an InfluxDB query with JSON encoded params for binding"""

        # TODO: this should be prettier once this issue is resolved
        # https://github.com/influxdata/influxdb-python/issues/603
        return self.client.query(query, params={"params": json.dumps(params)})
