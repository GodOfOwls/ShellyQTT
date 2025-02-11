import requests
import json
import os
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# InfluxDB Configuration
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORGANIZATION = os.getenv("INFLUXDB_ORGANIZATION")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Fronius Inverter Configuration
FRONIUS_INVERTER_IP = os.getenv("FRONIUS_INVERTER_IP")

# --- InfluxDB Client Setup ---
client = InfluxDBClient(url=f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}", token=INFLUXDB_TOKEN, org=INFLUXDB_ORGANIZATION)
write_api = client.write_api(write_options=SYNCHRONOUS)


def get_fronius_data(endpoint):
    """
    Fetches data from the Fronius Solar API.

    Args:
        endpoint: The specific API endpoint to query (e.g., "PowerFlowRealtimeData.fcgi").

    Returns:
        The JSON response from the API, or None if there's an error.
    """
    url = f"http://{FRONIUS_INVERTER_IP}/solar_api/v1/Get{endpoint}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Fronius API: {e}")
        return None


def write_data_to_influxdb(measurement_name, data):
    """
    Writes data to InfluxDB.

    Args:
        measurement_name: The name of the measurement in InfluxDB.
        data: A dictionary of data to write.
    """
    points = []
    timestamp = time.time_ns()

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (int, float)):
                point = Point(measurement_name).field(key, float(value)).time(timestamp, WritePrecision.NS).tag("inverter_ip",
                                                                                                         FRONIUS_INVERTER_IP)
                points.append(point)
            elif isinstance(value, dict):
                # Check for 'Value' and 'Unit' and handle accordingly
                if 'Value' in value and 'Unit' in value:
                    if value['Value'] is not None:
                        point = Point(measurement_name).field(key, float(value['Value'])).time(timestamp,
                                                                                               WritePrecision.NS).tag(
                            "inverter_ip", FRONIUS_INVERTER_IP)
                        points.append(point)


                else:
                    # Handle other nested dictionaries recursively
                    write_data_to_influxdb(measurement_name + "_" + key, value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        write_data_to_influxdb(measurement_name, item)
                    elif isinstance(item, (int, float)):
                        point = Point(measurement_name).field(measurement_name, float(item)).time(timestamp,
                                                                                                  WritePrecision.NS).tag(
                            "inverter_ip", FRONIUS_INVERTER_IP)
                        points.append(point)

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"Successfully wrote data to InfluxDB: {measurement_name}")
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")


def main():
    """
    Main function to fetch and store Fronius data.
    """
    endpoints = {
        "inverter_common": "InverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=CommonInverterData",
        #replace DeviceID=1 with your ID if needed
        "inverter_3P": "InverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=3PInverterData",
        "inverter_minmax": "InverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=MinMaxInverterData",
        #"storage": "StorageRealtimeData.cgi?Scope=Device&DeviceId=0", #replace DeviceID=0 with your ID if needed
        #"meter": "MeterRealtimeData.cgi?Scope=Device&DeviceId=0", #replace DeviceID=0 with your ID if needed
        #"powerflow": "PowerFlowRealtimeData.fcgi",
        #"system_common": "InverterRealtimeData.cgi?Scope=System&DataCollection=CommonInverterData",
        #"system_cumulation": "InverterRealtimeData.cgi?Scope=System&DataCollection=CumulationInverterData",
        #"system_minmax": "InverterRealtimeData.cgi?Scope=System&DataCollection=MinMaxInverterData",
    }

    while True:
        for measurement_name, endpoint in endpoints.items():
            fronius_data = get_fronius_data(endpoint)
            if fronius_data:
                if 'Body' in fronius_data and 'Data' in fronius_data['Body']:
                    write_data_to_influxdb(measurement_name, fronius_data['Body']['Data'])

        # Adjust the sleep time as needed, but be mindful of API rate limits
        time.sleep(60)


if __name__ == "__main__":
    main()
