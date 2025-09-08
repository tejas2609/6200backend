from datetime import datetime
from functools import partial
from typing import Optional
from fastapi import HTTPException
from firebase_admin import firestore, storage
import numpy as np
from scipy.signal import savgol_filter, butter, filtfilt
from scipy.integrate import cumulative_trapezoid
import gcsfs
import zarr
import asyncio
from app.services.websockets import send_live_update
import threading

db = firestore.client()
bucket = storage.bucket()
WATCHES = {}
WATCHES_LOCK = threading.Lock()

BASE_PATH = "project-8680797989633263399.firebasestorage.app/"

import os
key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../firebase-key.json"))

fs = gcsfs.GCSFileSystem(project="project-8680797989633263399", token=key_path)

OFFSET = 0.0003

def get_file_details(doc_id: str) -> str:
    try:
        doc_ref = db.collection("files").document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_data = doc.to_dict()
            return doc_data.get("storage_path", "") or ""
        return ""
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


def _zarr_path(filename: str) -> str:
    return f"{BASE_PATH}{filename}/SData"

def _open_zarr_group(filename: str):
    mapper = fs.get_mapper(_zarr_path(filename))
    try:
        return zarr.open_consolidated(mapper, mode="r")
    except Exception:
        return zarr.open_group(store=mapper, mode="r")

def _compute_time_slice(time_data: np.ndarray, duration) -> slice:
    time_data = time_data[np.isfinite(time_data)]
    if time_data.size == 0:
        return slice(0, 0)
    full_span = float(time_data[-1] - time_data[0])
    if time_data[0] > 10000000000:
        duration = int(duration) * 1000
    if duration and duration != "undefined":
        span = min(full_span, float(duration))
    else:
        span = full_span
    start_time = time_data[-1] - span 
    start_idx = int(np.searchsorted(time_data, start_time, side="left"))
    print(start_idx)
    return slice(start_idx, None)

def _estimate_fs(time_window: np.ndarray) -> float:
    t = np.asarray(time_window, dtype=float)
    t = t[np.isfinite(t)]
    if t.size < 2:
        return 1.0
    t = np.unique(t)
    diffs = np.diff(t)
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        return 1.0
    med_dt = float(np.median(diffs))
    return 1.0 / med_dt if med_dt > 0 else 1.0

def _normalize_cutoffs(lowcut: float, highcut: float, fs: float, margin: float = 1e-3):
    nyq = 0.5 * fs
    if nyq <= 0:
        raise ValueError("Non-positive Nyquist frequency. Check your time axis.")
    
    lo_hz, hi_hz = sorted((abs(lowcut), abs(highcut)))
    hi_hz = min(hi_hz, nyq * (1.0 - margin))
    lo_hz = max(lo_hz, 1e-6)

    if lo_hz >= hi_hz:
        lo_hz = max(1e-6, hi_hz * 0.5)

    wn_low = lo_hz / nyq
    wn_high = hi_hz / nyq

    wn_low = max(min(wn_low, 1.0 - 2e-6), 1e-6)
    wn_high = max(min(wn_high, 1.0 - 1e-6), wn_low + 1e-6)

    return wn_low, wn_high

def _butter_bandpass(lowcut, highcut, fs, order=4):
    wn_low, wn_high = _normalize_cutoffs(lowcut, highcut, fs)
    b, a = butter(order, [wn_low, wn_high], btype="band")
    return b, a

def _butter_lowpass(cutoff, fs, order=2):
    nyq = 0.5 * fs
    if nyq <= 0:
        raise ValueError("Non-positive Nyquist frequency. Check your time axis.")
    cutoff = min(abs(cutoff), nyq * 0.99)
    wn = max(min(cutoff / nyq, 1.0 - 1e-6), 1e-6)
    b, a = butter(order, wn, btype="low", analog=False)
    return b, a

def _safe_filtfilt(b, a, x, axis=0):
    n = x.shape[0]
    padlen = 3 * (max(len(a), len(b)) - 1)
    if n <= max(15, padlen):
        return x
    return filtfilt(b, a, x, axis=axis)

def _apply_filter(kind: str, data: np.ndarray, time_window: np.ndarray) -> np.ndarray:
    if data.size == 0 or kind == "":
        return data

    if kind == "savgol":
        n = data.shape[0]
        wl = min(201, n if n % 2 == 1 else n - 1)
        if wl < 7:
            wl = 7 if n >= 7 else (n | 1)
        po = 5 if wl > 5 else max(2, wl - 1)
        return savgol_filter(data, window_length=wl, polyorder=po, axis=0, mode="interp")

    fs = _estimate_fs(time_window)

    if kind == "bandpass":
        b, a = _butter_bandpass(0.5, 10.0, fs, order=4)
        return _safe_filtfilt(b, a, data, axis=0)

    if kind == "butterworth":
        b, a = _butter_lowpass(2.0, fs, order=2)
        return _safe_filtfilt(b, a, data, axis=0)

    return data

def get_time1(filename, duration, _unused_filter: str = ""):
    root = _open_zarr_group(filename)
    time_data = root["Time"][:]
    s = _compute_time_slice(time_data, duration)
    return time_data[s].astype(float).tolist()

def extract_columns1(filename, colname: str, duration, filt: str = "", applyFilter: bool = True):
    if not filename:
        return []

    root = _open_zarr_group(filename)

    time_arr = root["Time"][:]
    s = _compute_time_slice(time_arr, duration)
    time_window = time_arr[s].astype(float)

    if colname != "RawVelocity":
        if colname not in root:
            raise HTTPException(status_code=404, detail=f"Dataset '{colname}' not found in Zarr group")
        data = root[colname][s].astype(float)
        print(data)
        data = data[np.isfinite(data)]
        if applyFilter:
            kind = filt
            data = _apply_filter(kind, data, time_window)
        return data.tolist()

    rv = root["RawVelocity"][s, :30].astype(float)  # shape: (T, 30)
    rv[~np.isfinite(rv)] = 0.0
    disp = cumulative_trapezoid(rv, time_window[:, None], axis=0, initial=0.0)
    if applyFilter:
        kind = filt
        disp = _apply_filter(kind, disp, time_window)
    offsets = np.arange(disp.shape[1]) * OFFSET
    disp = disp + offsets
    return [disp[:, i].tolist() for i in range(disp.shape[1])]

def get_patients_general_data(filename):
    root = _open_zarr_group(filename)
    return {"metadata": dict(root.attrs), "vitals": list(root.keys())}

def on_snapshot(col_snapshot, changes, read_time, x_axis, y_axis):
    output = {"x_columns": [], "y_columns": []}
    for doc in col_snapshot:
        data = doc.to_dict()
        if y_axis in data:
            series = data.get(y_axis, [])
            for d in series:
                output["x_columns"].append(d.get("timestamp", 0))
                output["y_columns"].append(d.get("value", 0))
    if output["x_columns"] and output["y_columns"]:
        upload_id = f"live_{y_axis}"
        output["x_columns"] = output["x_columns"][-10:]
        output["y_columns"] = output["y_columns"][-10:]
        asyncio.run(send_live_update(upload_id, {"output_data": output, "status": "success"}))

def onsnapshot_heatmap(col_snapshot, changes, read_time, x_axis, patientName):
    output = {"x_columns": [], "y_columns": []}
    obj = {}
    time_obj = []
    for doc in col_snapshot:
        data = doc.to_dict()
        for item, values in data.items():
            obj[item] = [v.get("value", 0) for v in values[-10:]]
            if not len(time_obj):
                time_obj = [v.get("timestamp", 0) for v in values[-10:]]
    output["y_columns"] = obj
    output["x_columns"] = time_obj
    upload_id = f"live_heatmap_{patientName}"
    asyncio.run(send_live_update(upload_id, {"output_data": output, "status": "success"}))
    
def get_watch_key(hospital_name: str, patient_name: str, heat_map: bool) -> str:
    return f"{hospital_name}:{patient_name}:{int(bool(heat_map))}"

def get_live_data(xaxis, yaxis, hospital_name, patientName, heatMap):
    try:
        key = get_watch_key(hospital_name, patientName, heatMap)
        col_ref = db.collection("live_data").document(hospital_name).collection("patients").document(patientName)
        # with WATCHES_LOCK:
        #     if key in WATCHES:
        #         return
        if heatMap:
            callback = col_ref.on_snapshot(partial(onsnapshot_heatmap, x_axis=xaxis, patientName=patientName))
        else:
            callback = col_ref.on_snapshot(partial(on_snapshot, x_axis=xaxis, y_axis=yaxis)) 
            # watch = col_ref.on_snapshot(callback)
            # WATCHES[key] = watch
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving live data: {str(e)}")

def store_notification(email: Optional[str], role: Optional[str], type: Optional[str], hospital: Optional[str]):
    notification_ref = db.collection('notifications')
    now = datetime.utcnow().isoformat()
    notification_obj = {
        'read': False,
        'email': email,
        'role': role,
        'type': type,
        'created_at': now
    }
    if hospital:
        notification_obj['hospital'] = hospital
    notif_doc_ref = notification_ref.document()
    notification_obj['id'] = notif_doc_ref.id
    notif_doc_ref.set(notification_obj)
    