import asyncio
import json
from kafka import KafkaConsumer
from firebase_admin import firestore
from functools import partial
import threading, time
from fastapi import HTTPException
from google.cloud.firestore_v1 import DocumentSnapshot

from app.services.websockets import send_live_update

db = firestore.client()

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "vitals_data"

def start_storage_writer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="data_storage"
    )

    for msg in consumer:
        try:
            payload = msg.value
            hospital = payload.get("hospital")
            patient = payload.get("patient")
            batch = payload.get("data", [])

            if not hospital or not patient or not batch:
                continue

            doc_ref = (
                db.collection("live_data")
                .document(hospital)
                .collection("patients")
                .document(patient)
            )

            updates = {}
            for entry in batch:
                timestamp = entry.get("timestamp")
                if not timestamp:
                    continue

                for vital, value in entry.items():
                    if vital == "timestamp":
                        continue
                    updates.setdefault(vital, []).append({"value": value, "timestamp": timestamp})

            for vital, val_array in updates.items():
                doc_ref.set(
                    {vital: firestore.ArrayUnion(val_array)},
                    merge=True
                )

        except Exception as e:
            print(f"[ERROR] Failed to store batch: {e}")


WATCHES = {}
WATCHES_LOCK = threading.Lock()
LAST_UPDATE_TIME = {}   
LAST_EMIT_TS = {}     

def _should_emit(doc: DocumentSnapshot, ref_id: str, debounce_s: float = 0.20) -> bool:
    path = doc.reference.path
    ut = getattr(doc, "update_time", None)
    if ut is not None:
        prev_ut = LAST_UPDATE_TIME.get(path)
        if prev_ut == ut:
            return False
        LAST_UPDATE_TIME[path] = ut
    now = time.monotonic()
    last = LAST_EMIT_TS.get(ref_id, 0.0)
    if (now - last) < debounce_s:
        return False
    LAST_EMIT_TS[ref_id] = now
    return True

def get_watch_key(hospital_name: str, patient_name: str, heat_map: bool) -> str:
    return f"{hospital_name}:{patient_name}:{1 if heat_map else 0}"

# ---- callbacks ----
def on_snapshot(doc_snapshot, changes, read_time, x_axis, y_axis):
    for doc in doc_snapshot:
        if not doc.exists:
            continue
        ref_id = f"live_{y_axis}"
        if not _should_emit(doc, ref_id):
            continue

        data = doc.to_dict() or {}
        series = data.get(y_axis, [])
        x_cols, y_cols = [], []
        for d in series[-10:]:
            x_cols.append(d.get("timestamp", 0))
            y_cols.append(d.get("value", 0))
        if x_cols and y_cols:
            asyncio.run(send_live_update(ref_id, {
                "output_data": {"x_columns": x_cols, "y_columns": y_cols},
                "status": "success"
            }))

def onsnapshot_heatmap(doc_snapshot, changes, read_time, x_axis, patientName):
    for doc in doc_snapshot:
        if not doc.exists:
            continue
        ref_id = f"live_heatmap_{patientName}"
        if not _should_emit(doc, ref_id):
            continue

        data = doc.to_dict() or {}
        latest = {}
        for vital, values in data.items():
            if isinstance(values, list) and values:
                latest[vital] = values[-1].get("value", 0)

        if latest:
            x_cols = list(latest.keys())
            y_cols = list(latest)  
            asyncio.run(send_live_update(ref_id, {
                "output_data": {"x_columns": x_cols, "y_columns": y_cols},
                "status": "success"
            }))

def get_live_data(xaxis, yaxis, hospital_name, patientName, heatMap):
    try:
        key = get_watch_key(hospital_name, patientName, heatMap)
        col_ref = db.collection("live_data") \
                    .document(hospital_name) \
                    .collection("patients") \
                    .document(patientName)

        with WATCHES_LOCK:
            if key in WATCHES:
                return

            if heatMap:
                callback = partial(onsnapshot_heatmap, x_axis=xaxis, patientName=patientName)
            else:
                callback = partial(on_snapshot, x_axis=xaxis, y_axis=yaxis)
            watch = col_ref.on_snapshot(callback)
            WATCHES[key] = watch
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving live data: {str(e)}")