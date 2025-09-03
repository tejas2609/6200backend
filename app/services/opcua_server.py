from opcua import Server
import random
import time
import logging

ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
NAMESPACE_URI = "http://medical.example.com"
# For faster testing set to 0.02 (50 Hz). Your current pipeline uses 0.1 s.
UPDATE_INTERVAL = 1  # seconds

VITAL_RANGES = {
    "HeartRate": (50, 150),
    "CO2": (20, 140),
    "BloodPressure": (80, 160),
}

logging.getLogger("opcua").setLevel(logging.WARNING)
logging.getLogger("opcua.uaprotocol").setLevel(logging.WARNING)
logging.getLogger("opcua.client.ua_client").setLevel(logging.WARNING)


def generate_vital_value(min_val, max_val):
    return round(random.uniform(min_val, max_val), 2)

def create_patient_node(objects, idx, patient_name):
    """Create OPC UA node for a patient and return variables dict."""
    patient = objects.add_object(idx, patient_name)
    variables = {}
    for vital, (vmin, vmax) in VITAL_RANGES.items():
        var = patient.add_variable(idx, vital, generate_vital_value(vmin, vmax))
        var.set_writable()
        variables[vital] = var
    return variables

def start_opcua_server():
    server = Server()
    server.set_endpoint(ENDPOINT)
    idx = server.register_namespace(NAMESPACE_URI)

    objects = server.get_objects_node()

    patients_data = {
        # add more patients as needed
        "Patient4Vitals": create_patient_node(objects, idx, "Patient4Vitals"),
    }

    server.start()
    print(f"✅ OPC UA Server running at {ENDPOINT}")

    try:
        while True:
            for _, vitals in patients_data.items():
                for vital, (vmin, vmax) in VITAL_RANGES.items():
                    vitals[vital].set_value(generate_vital_value(vmin, vmax))
            time.sleep(UPDATE_INTERVAL)
    except KeyboardInterrupt:
        print("🛑 Shutting down OPC UA server...")
        server.stop()

if __name__ == "__main__":
    start_opcua_server()
