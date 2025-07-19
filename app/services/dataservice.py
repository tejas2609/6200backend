from fastapi import HTTPException
from scipy.io import loadmat
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
from firebase_admin import firestore, storage
import os
from scipy.integrate import cumulative_trapezoid
from scipy.signal import butter, filtfilt

db = firestore.client()
bucket = storage.bucket()

OFFSET = 3
# Load the .mat file

def downloadFile(filename: str):
    tmp_dir = os.getenv("DOWNLOAD_FOLDER", "D:/UoS/COMP6200/firebase/app/tmp")
    os.makedirs(tmp_dir, exist_ok=True) 
    local_filename = os.path.join(tmp_dir, filename)
    print(local_filename, os.path.isfile(local_filename))
    
    if not os.path.isfile(local_filename) or os.path.getsize(local_filename) == 0: 
        blob = bucket.blob(filename)
        blob.download_to_filename(local_filename)
        if os.path.isfile(local_filename) and os.path.getsize(local_filename) > 0:
            return {'status': 'success', 'file_path': local_filename}
        else:
            return {'status': 'failure', 'file_path': local_filename}
    else:
        return {'status': 'success', 'file_path': local_filename}


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
        

# Function to extract and smooth physiological data (e.g., Bp)
def extract_structure_safe(colname: str, sdata) -> pd.DataFrame:   
    bp_data = sdata[colname][0, 0][0, 0].flatten()
    bp_data = bp_data[np.isfinite(bp_data)]

    if len(bp_data) >= 0:
        smoothed = savgol_filter(bp_data, window_length=201, polyorder=5)
    else:
        smoothed = bp_data

    return smoothed.tolist()

# Function to get time values
def get_time(filename) -> pd.DataFrame:
    mat = loadmat(filename)
    sdata = mat["SData"]
    time_data = sdata['Time'][0, 0].flatten()
    time_data = time_data[np.isfinite(time_data)]
    return time_data.tolist()

# === Example usage ===
def extract_columns(fileid, colname: str):
    filename = get_file_details(fileid)
    if filename:
        downloadstatus = downloadFile(filename)
        if downloadstatus['status'] == 'success':    
            mat = loadmat(downloadstatus['file_path'])
            sdata = mat["SData"]        
            if colname == 'RawVelocity':
                data =  getRawVelocity(sdata)
                return data, downloadstatus['file_path']
            else:
                bp_df = extract_structure_safe(colname, sdata)
                return bp_df, downloadstatus['file_path']

def bandpass(data, lowcut=0.5, highcut=50, fs=500, order=4):
    nyquist = 0.5 * fs
    low = lowcut / nyquist
    high = highcut / nyquist
    b, a = butter(order, [low, high], btype='band')
    return filtfilt(b, a, data)

def getRawVelocity(sData):
    sdata = sData[0, 0]
    raw_velocity_matrix = sdata["RawVelocity"][0]   # final (50000, 30) array
    result = []
    
    time = sdata['Time'].flatten()[0: 15000]
    for i in range(30):
        l = raw_velocity_matrix[0][:, i].tolist()[0: 15000]
        displacement = cumulative_trapezoid(l, time, initial=0).tolist()
        filtered_displacement = bandpass(displacement) 
        # centered_displacement = filtered_displacement - np.min(filtered_displacement)
        # waterfall_displacement = centered_displacement + i * OFFSET
        # result.append(waterfall_displacement.tolist())
        result.append(filtered_displacement.tolist())
    return result


def extract_col_names(filename, prefix=""):

    mat = loadmat(filename)
    sdata = mat["SData"]
    obj = sdata[0]
    structure = {}
    
    if hasattr(obj, 'dtype') and obj.dtype.names:  # MATLAB struct
        for name in obj.dtype.names:
            try:
                val = obj[name]
                while isinstance(val, np.ndarray) and val.size == 1:
                    val = val[0]
                full_name = f"{prefix}.{name}" if prefix else name
                if hasattr(val, 'dtype') and val.dtype.names:
                    structure.update(extract_structure_safe(val, sdata, prefix=full_name))
                else:
                    structure[full_name] = val.shape if hasattr(val, 'shape') else type(val)
            except Exception as e:
                structure[full_name] = f"Error: {e}"
    else:
        structure[prefix] = obj.shape if hasattr(obj, 'shape') else type(obj)
    return structure