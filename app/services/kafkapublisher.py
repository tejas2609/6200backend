import threading
import time
import json
from datetime import datetime
from opcua import Client
from kafka import KafkaProducer
import logging

# keep your conversion util
from app.services.conversion import convert_to_zarr_live_data

OPC_UA_ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
KAFKA_BROKER = "localhost:9092"
# Batch (for storage/analytics)
KAFKA_TOPIC = "vitals_data"
# Point stream (for low-latency alerts)
POINTS_TOPIC = "vitals_points"

HOSPITAL = "General Hospital"
# Producer interval should match OPC UA server update rate.
INTERVAL = 0.1
BATCH_SIZE = int(5 / INTERVAL)  # 5-second batches

logging.getLogger("opcua").setLevel(logging.WARNING)
logging.getLogger("opcua.uaprotocol").setLevel(logging.WARNING)
logging.getLogger("opcua.client.ua_client").setLevel(logging.WARNING)

def get_all_patients(client):
    root = client.get_objects_node()
    patients = {}
    for child in root.get_children():
        name = child.get_browse_name().Name
        if name.endswith("Vitals"):
            # variables directly under the patient object
            vitals = {v.get_browse_name().Name: v for v in child.get_children()}
            patients[name] = vitals
    if not patients:
        raise Exception("❌ No patient vitals nodes found.")
    return patients

def patient_worker(patient_name, vitals, producer):
    buffer = []
    while True:
        try:
            ts = int(datetime.utcnow().timestamp() * 1000)
            point = {"timestamp": ts}
            for vital_name, node in vitals.items():
                # numeric values are expected for alerts. If complex, cast accordingly.
                point[vital_name] = node.get_value()

            # 1) publish a single-point message for alerts
            point_msg = {
                "hospital": HOSPITAL,
                "patient": patient_name,
                "timestamp": ts,
                "data": point
            }
            producer.send(POINTS_TOPIC, point_msg)

            # 2) build batch for storage/analytics
            buffer.append(point)
            if len(buffer) >= BATCH_SIZE:
                batch_msg = {
                    "hospital": HOSPITAL,
                    "patient": patient_name,
                    "timestamp": ts,
                    "data": buffer
                }
                producer.send(KAFKA_TOPIC, batch_msg)
                buffer.clear()

            time.sleep(INTERVAL)

        except Exception:
            # fall back to local capture, then retry after a short delay
            convert_to_zarr_live_data(patient_name)
            time.sleep(5)

def start_producing():
    backoff = 1
    while True:
        try:
            client = Client(OPC_UA_ENDPOINT)
            client.connect()

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=5,  # coalesce tiny bursts
                acks="all",
            )

            patients = get_all_patients(client)

            threads = []
            for patient_name, vitals in patients.items():
                t = threading.Thread(target=patient_worker, args=(patient_name, vitals, producer), daemon=True)
                t.start()
                threads.append(t)

            print(f"✅ Producer running for patients: {list(patients.keys())}")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Stopping...")
            finally:
                client.disconnect()
                print("Disconnected from OPC UA server")
                return
        except Exception:
            # exponential backoff reconnect
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    start_producing()
