from fastapi import HTTPException
from scipy.io import loadmat
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
from firebase_admin import firestore, storage
import os
from scipy.integrate import cumulative_trapezoid
from scipy.signal import butter, filtfilt
import gcsfs
import zarr

db = firestore.client()
bucket = storage.bucket()

OFFSET = 0.0005
key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../firebase-key.json"))
fs = gcsfs.GCSFileSystem(project='project-8680797989633263399', token=key_path)

BASE_PATH = 'project-8680797989633263399.firebasestorage.app/'


def get_file_details(doc_id: str):
    try:
        doc_ref = db.collection('files').document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_data = doc.to_dict()
            doc_name = doc_data.get('storage_path')
            # doc_ref.delete()
            return doc_name

        return ''
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

def extract_structure_safe1(root, colname: str, filename, duration):
    time = get_time1(filename, duration)
    bp_data = root[colname][50000 - len(time): 50000]
    bp_data = bp_data[np.isfinite(bp_data)]
    if len(bp_data) >= 0:
        smoothed = savgol_filter(bp_data, window_length=201, polyorder=5)
    else:
        smoothed = bp_data
    return smoothed.tolist()

def get_time1(filename, duration):
    zarr_path = BASE_PATH + filename + '/SData'
    mapper = fs.get_mapper(zarr_path)
    root = zarr.open_group(store=mapper, mode='r')

    time_data = root['Time'][:]
    final_duration = int(time_data[-1] - time_data[0])
    if duration and duration != 'undefined':
        final_duration = int(final_duration) if final_duration < float(duration) else int(duration)
    else:
        if final_duration > 10:
            final_duration = 10
    start_time = time_data[-1] - final_duration
    start_idx = np.searchsorted(time_data, start_time, side="left")
    time_data = time_data[start_idx:]
    time_data = time_data[np.isfinite(time_data)]
    return time_data.tolist()


def extract_columns1(filename, colname: str, duration):
    if filename:
        zarr_path = BASE_PATH + filename + '/SData'
        mapper = fs.get_mapper(zarr_path)
        root = zarr.open_group(store=mapper, mode='r')
        if colname != 'RawVelocity':
            data = extract_structure_safe1(root, colname, filename, duration)
            return data
        else:
            data = getRawVelocity1(root, filename, duration)
            return data

def bandpass(data, lowcut=0.5, highcut=10, fs=500, order=4):
    data = np.asarray(data).flatten()
    nyquist = 0.5 * fs
    low = lowcut / nyquist
    high = highcut / nyquist
    b, a = butter(order, [low, high], btype='band')
    return filtfilt(b, a, data)

def getRawVelocity1(root, filename, duration):
    raw_velocity_matrix = root["RawVelocity"][:]
    result = []    
    time = get_time1(filename, duration)
    
    for i in range(30):
        l = raw_velocity_matrix[:, i].tolist()[50000 - len(time): 50000]
        displacement = cumulative_trapezoid(l, time, initial=0).tolist()
        filtered_displacement = bandpass(displacement) 
        filtered_displacement = np.array(filtered_displacement) + i * OFFSET
        result.append(filtered_displacement.tolist())
    return result
