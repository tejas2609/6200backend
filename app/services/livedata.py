import threading
import random, time
from firebase_admin import firestore, storage

lock = threading.Lock()
buffer = []
db = firestore.client()
bucket = storage.bucket()


doc_ref = db.collection("live_data").document("co2_log")
if not doc_ref.get().exists:
    doc_ref.set({"CO2": []})
    print("Created new document: co2_log with empty CO2 array.")
    
def generate_data():
    print('started generating')
    while True:
        value = random.randint(30, 180)
        with lock:
            buffer.append(value)
        time.sleep(1)

def flush_to_firestore():
    """Flush buffer to MongoDB every 10 seconds by appending to existing CO2 array."""
    while True:
        time.sleep(10)
        with lock:
            if buffer:
                try:
                    doc_ref.update({
                        "CO2": firestore.ArrayUnion(buffer)
                    })
                    buffer.clear()
                except Exception as e:
                    print("Error updating Firestore:", e)