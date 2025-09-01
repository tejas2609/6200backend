from opcua import Server
import random
import time

# Configuration
ENDPOINT = "opc.tcp://localhost:4840/freeopcua/server/"
NAMESPACE_URI = "http://medical.example.com"
UPDATE_INTERVAL = 0.1  # seconds

VITAL_RANGES = {
    "HeartRate": (0, 180),
    "CO2": (30, 150),
    "BloodPressure": (30, 150),
}

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
    
    # Create multiple patient nodes
    patients_data = {
        # "Patient1Vitals": create_patient_node(objects, idx, "Patient1Vitals"),
        # "Patient2Vitals": create_patient_node(objects, idx, "Patient2Vitals"),
        # "Patient3Vitals": create_patient_node(objects, idx, "Patient3Vitals"),
        "Patient4Vitals": create_patient_node(objects, idx, "Patient4Vitals"),
        # "Patient5Vitals": create_patient_node(objects, idx, "Patient5Vitals"),
        # "Patient6Vitals": create_patient_node(objects, idx, "Patient6Vitals"),
        # "Patient7Vitals": create_patient_node(objects, idx, "Patient7Vitals"),
        # "Patient8Vitals": create_patient_node(objects, idx, "Patient8Vitals"),
        # "Patient9Vitals": create_patient_node(objects, idx, "Patient9Vitals")
    }

    server.start()
    print(f"✅ OPC UA Server running at {ENDPOINT}")

    try:
        while True:
            for patient_name, vitals in patients_data.items():
                for vital, (vmin, vmax) in VITAL_RANGES.items():
                    vitals[vital].set_value(generate_vital_value(vmin, vmax))
            time.sleep(UPDATE_INTERVAL)
    except KeyboardInterrupt:
        print("🛑 Shutting down OPC UA server...")
        server.stop()

if __name__ == "__main__":
    start_opcua_server()
