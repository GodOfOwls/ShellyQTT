import paho.mqtt.client as mqtt
import os

import json

from dotenv import load_dotenv
import influxdb_client

from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()
##
# For BLU Devices use https://github.com/iobroker-community-adapters/ioBroker.shelly/blob/master/docs/en/ble-devices.md
##
# Read values from .env file
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT'))
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORGANIZATION = os.getenv('INFLUXDB_ORGANIZATION')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

# Initialize InfluxDB client
influx_client = influxdb_client.InfluxDBClient(
    url=f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}",
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORGANIZATION
)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribing to all Shelly topics
    client.subscribe("#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    if msg.topic.startswith("shelly"):

        topic_parts = msg.topic.split('/')
        if len(topic_parts) >= 3:
            device_type = topic_parts[0]
            device_update = topic_parts[1]
            topic = "/".join(topic_parts[2:])
            # We are only interested in 'status' topics for each device
            if 'status' in device_update:
                # We are filtering messages based on device type
                if 'switch:0' in topic:
                    payload = msg.payload.decode("utf-8")

                    try:
                        data = json.loads(payload)

                        # Extract relevant fields
                        id = device_type

                        output = data.get("output")
                        apower = data.get("apower")
                        voltage = data.get("voltage")
                        current = data.get("current")
                        total_energy = data["aenergy"].get("total")
                        temperature_c = data["temperature"].get("tC")
                        temperature_f = data["temperature"].get("tF")

                        # Create an InfluxDB point
                        point = Point("shelly_power") \
                            .tag("device_id", id) \
                            .field("output", output) \
                            .field("apower", apower) \
                            .field("voltage", voltage) \
                            .field("current", current) \
                            .field("total_energy", total_energy) \
                            .field("temperature_c", temperature_c) \
                            .field("temperature_f", temperature_f)

                        # Write to InfluxDB
                        with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                            writer.write(org=INFLUXDB_ORGANIZATION,
                                         bucket=INFLUXDB_BUCKET,
                                         record=point,
                                         )
                        print(f"Data written to InfluxDB: {point}")

                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON payload: {payload}")

                elif 'temperature:0' in topic:
                    payload = msg.payload.decode("utf-8")

                    try:
                        data = json.loads(payload)

                        # Extract relevant fields
                        id = device_type

                        temperature_c = data.get("tC")
                        temperature_f = data.get("tF")

                        # Create an InfluxDB point
                        point = Point("shelly_temperature") \
                            .tag("device_id", id) \
                            .field("temperature_c", temperature_c) \
                            .field("temperature_f", temperature_f)

                        # Write to InfluxDB
                        with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                            writer.write(org=INFLUXDB_ORGANIZATION,
                                         bucket=INFLUXDB_BUCKET,
                                         record=point,
                                         )
                        print(f"Data written to InfluxDB: {point}")

                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON payload: {payload}")

                elif 'humidity:0' in topic:
                    payload = msg.payload.decode("utf-8")

                    try:
                        data = json.loads(payload)

                        # Extract relevant fields
                        id = device_type

                        humidity = data.get("rh")

                        # Create an InfluxDB point
                        point = Point("shelly_humidity") \
                            .tag("device_id", id) \
                            .field("humidity", humidity)

                        # Write to InfluxDB
                        with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                            writer.write(org=INFLUXDB_ORGANIZATION,
                                         bucket=INFLUXDB_BUCKET,
                                         record=point,
                                         )
                        print(f"Data written to InfluxDB: {point}")

                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON payload: {payload}")

            elif 'events' in device_update:
                if len(topic_parts) >= 3 and topic_parts[2] == 'ble':
                    data = msg.payload.decode("utf-8")
                    data = json.loads(data)
                    data = data.get("payload")

                    if 'motion' in data:

                        point = Point("motion_sensor") \
                            .field("encryption", data.get("encryption")) \
                            .field("BTHome_version", data.get("BTHome_version")) \
                            .field("pid", data.get("pid")) \
                            .field("battery", data.get("battery")) \
                            .field("temperature", data.get("temperature")) \
                            .field("illuminance", data.get("illuminance")) \
                            .field("motion", data.get("motion")) \
                            .field("rssi", data.get("rssi")) \
                            .tag("address", data.get("address"))

                        with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                            writer.write(org=INFLUXDB_ORGANIZATION,
                                         bucket=INFLUXDB_BUCKET,
                                         record=point,
                                         )
                        print(f"Data written to InfluxDB: {point}")

                    elif 'button' in data:

                        point = Point("button") \
                            .field("encryption", data.get("encryption")) \
                            .field("BTHome_version", data.get("BTHome_version")) \
                            .field("pid", data.get("pid")) \
                            .field("battery", data.get("battery")) \
                            .field("button", data.get("button")) \
                            .field("rssi", data.get("rssi")) \
                            .tag("address", data.get("address"))

                        with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                            writer.write(org=INFLUXDB_ORGANIZATION,
                                         bucket=INFLUXDB_BUCKET,
                                         record=point,
                                         )
                        print(f"Data written to InfluxDB: {point}")

                    elif 'window' in data:

                        point = Point("door") \
                            .field("encryption", data.get("encryption")) \
                            .field("BTHome_version", data.get("BTHome_version")) \
                            .field("pid", data.get("pid")) \
                            .field("battery", data.get("battery")) \
                            .field("illuminance", data.get("illuminance")) \
                            .field("window", data.get("window")) \
                            .field("rotation", data.get("rotation")) \
                            .field("rssi", data.get("rssi")) \
                            .tag("address", data.get("address"))

                        with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                            writer.write(org=INFLUXDB_ORGANIZATION,
                                         bucket=INFLUXDB_BUCKET,
                                         record=point,
                                         )
                        print(f"Data written to InfluxDB: {point}")
    elif msg.topic.startswith('shellies'):

        topic_parts = msg.topic.split('/')
        if len(topic_parts) >= 4:
            device_type = topic_parts[1]
            device_update = topic_parts[2]
            topic = "/".join(topic_parts[3:])
            if topic == 'temperature':
                payload = msg.payload.decode("utf-8")
                point = Point("shelly_temperature") \
                    .tag("device_id", device_type) \
                    .field("temperature_c", float(payload))

                # Write to InfluxDB
                with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                    writer.write(org=INFLUXDB_ORGANIZATION,
                                 bucket=INFLUXDB_BUCKET,
                                 record=point,
                                 )
                print(f"Data written to InfluxDB: {point}")
                pass
            elif topic == 'humidity':
                payload = msg.payload.decode("utf-8")
                point = Point("shelly_humidity") \
                    .tag("device_id", device_type) \
                    .field("humidity", float(payload))

                # Write to InfluxDB
                with influx_client.write_api(write_options=SYNCHRONOUS) as writer:
                    writer.write(org=INFLUXDB_ORGANIZATION,
                                 bucket=INFLUXDB_BUCKET,
                                 record=point,
                                 )
                print(f"Data written to InfluxDB: {point}")
                pass


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

client.connect("pc-owl-03.ad.ebg.pw", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
client.loop_forever()
