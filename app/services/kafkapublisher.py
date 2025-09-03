import threading
import time
import json
from datetime import datetime
from opcua import Client
from kafka import KafkaProducer

from app.services.conversion import convert_to_zarr_live_data

OPC_UA_ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "vitals_data"
HOSPITAL = "General Hospital"
INTERVAL = 0.1 
BATCH_SIZE = int(5 / INTERVAL) 

def get_all_patients(client):
    root = client.get_objects_node()
    patients = {}
    for child in root.get_children():
        name = child.get_browse_name().Name
        if name.endswith("Vitals"):
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
            entry = {"timestamp": ts}
            for vital_name, node in vitals.items():
                entry[vital_name] = node.get_value()
            buffer.append(entry)

            if len(buffer) == BATCH_SIZE:
                message = {
                    "hospital": HOSPITAL,
                    "patient": patient_name,
                    "timestamp": ts,
                    "data": buffer
                }
                producer.send(KAFKA_TOPIC, message)
                buffer.clear()

            time.sleep(INTERVAL)

        except Exception as e:
            convert_to_zarr_live_data(patient_name)
            time.sleep(5) 

def start_producing():
    try:
        client = Client(OPC_UA_ENDPOINT)
        client.connect()
    
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    
        patients = get_all_patients(client)

        threads = []
        for patient_name, vitals in patients.items():
            t = threading.Thread(target=patient_worker, args=(patient_name, vitals, producer))
            t.daemon = True
            t.start()
            threads.append(t)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            client.disconnect()
            print("Disconnected from OPC UA server")
    except Exception as e:
        start_producing()
