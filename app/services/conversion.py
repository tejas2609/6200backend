from fastapi import APIRouter, BackgroundTasks, WebSocket, WebSocketDisconnect
from scipy.io import loadmat, savemat
import zarr
import numpy as np
import os
from firebase_admin import firestore, storage
import uuid
from datetime import datetime    
import shutil
import threading
import zipfile
import tempfile

router = APIRouter()

db = firestore.client()
bucket = storage.bucket()


def mat_file_conversion(mat_file_path, output_dir):
    mat_data = loadmat(mat_file_path, squeeze_me=True, struct_as_record=False)
    base_name = os.path.splitext(os.path.basename(mat_file_path))[0]
    zarr_folder = os.path.join(output_dir, base_name + '.zarr')

    os.makedirs(output_dir, exist_ok=True)
    root = zarr.open_group(zarr_folder, mode='w')

    convert_to_zarr(mat_data, root)
    return zarr_folder, base_name

def save_struct(group, struct_obj):
    for field in struct_obj._fieldnames:
        val = getattr(struct_obj, field)

        if isinstance(val, np.ndarray):
            if val.dtype == 'object':
                for i, item in enumerate(val):
                    if hasattr(item, '_fieldnames'):
                        subgrp = group.create_group(f"{field}_{i}")
                        save_struct(subgrp, item)
                    else:
                        group.create_dataset(field, data=val, shape=val.shape, overwrite=True)
            else:
                group.create_dataset(field, data=val, shape=val.shape, overwrite=True)
        elif hasattr(val, '_fieldnames'):
            subgrp = group.create_group(field)
            save_struct(subgrp, val)
        elif isinstance(val, (int, float, str, np.number)):
            group.attrs[field] = val

def convert_to_zarr(mat_data, root):
    for varname, val in mat_data.items():
        if varname.startswith('__'):
            continue
        if isinstance(val, np.ndarray):
            root.create_dataset(varname, data=val, shape=val.shape, overwrite=True)
        if hasattr(val, '_fieldnames'):
            group = root.create_group(varname)
            save_struct(group, val)
        elif isinstance(val, (int, float, str, np.number)):
            root.attrs[varname] = val
            


def convert_to_zarr_live_data(patient_name, upload = True):
    try:
        background_tasks = BackgroundTasks()
        doc_ref_live_data = db.collection("live_data").document("General Hospital") \
        .collection("patients").document(patient_name)
        doc_live_data = doc_ref_live_data.get()
        
        if not doc_live_data.exists:
            raise ValueError(f"Patient {patient_name} not found in Firestore.")
        
        data = doc_live_data.to_dict()
        if len(data) == 0:
            return
        zarr_path = os.path.join(os.getenv('DOWNLOAD_FOLDER', "D:/UoS/COMP6200/firebase/app/tmp"), patient_name + '.zarr')
        unique_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        filename = f"{timestamp}_{unique_id}_{patient_name}"
        
        if os.path.exists(zarr_path):
            shutil.rmtree(zarr_path) 
        
        root = zarr.open_group(zarr_path, mode='w')
        sdata = root.create_group("SData")
        time_field_created = False
        for vital, records in data.items():
            if not isinstance(records, list):
                continue
            records = [entry for entry in records if isinstance(entry, dict) and "value" in entry and "timestamp" in entry]
            if len(records) == 0:    
                continue
            records = sorted(records, key=lambda x: x["timestamp"])
            values = np.array([entry["value"] for entry in records], dtype=np.float32)
            if values.shape[0] == 0:
                continue
            sdata.create_dataset(vital, data=values, shape=values.shape)
            if not time_field_created:
                timestamps = np.array([entry["timestamp"] for entry in records], dtype=np.int64)
                sdata.create_dataset(f"Time", data=timestamps, shape=timestamps.shape)
                time_field_created = True
        if not upload:
            return root, zarr_path        
        if upload:
            doc_ref_live_data.delete()    
            doc_ref = db.collection("files").document(unique_id)
            doc_ref.set({
                "status": "in_progress",
                "progress": 0,
                "filename": filename,
                "user": "no-user",
                "hospital": "General Hospital",
                "timestamp": datetime.utcnow()
            })

            threading.Thread(target=upload_with_socket_progress1, args=(doc_ref, filename, zarr_path, unique_id)).start()
    except Exception as e:
        print(f"Error converting live data to Zarr: {e}")
        raise e
    
connections = {}

@router.websocket("/ws/{upload_id}")
async def websocket_endpoint(websocket: WebSocket, upload_id: str):
    await websocket.accept()
    connections[upload_id] = websocket
    try:
        while True:
            await websocket.receive_text()  # keep alive
    except WebSocketDisconnect:
        del connections[upload_id]
    
async def send_progress(upload_id: str, progress: float):
    ws = connections.get(upload_id)
    if ws:
        try:
            await ws.send_json({"progress": progress})
        except:
            pass

def upload_with_socket_progress1(doc_ref, filename, local_path, upload_id):
    import asyncio

    try:
        total_size = 0
        file_list = []
        # Collect all file paths and total size
        for root, _, files in os.walk(local_path):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, local_path).replace("\\", "/")
                blob_path = f"{filename}/{rel_path}"
                file_list.append((full_path, blob_path))
                total_size += os.path.getsize(full_path)
        uploaded = 0
        chunk_size = 1024 * 1024

        # Upload each file
        for full_path, blob_path in file_list:
            blob = bucket.blob(blob_path)
            with open(full_path, "rb") as f:
                writer = blob.open("wb", content_type="application/octet-stream")
                while chunk := f.read(chunk_size):
                    writer.write(chunk)
                    uploaded += len(chunk)
                    progress = round((uploaded / total_size) * 100, 2)
                    doc_ref.update({"progress": progress})
                    asyncio.run(send_progress(upload_id, progress))
                writer.close()

        doc_ref.update({
            "status": "success",
            "storage_path": filename
        })
        asyncio.run(send_progress(upload_id, 100))

    except Exception as e:
        doc_ref.update({"status": "failed", "error": str(e)})
        asyncio.run(send_progress(upload_id, -1))

    finally:
        print(';Done')
        # Remove local folder after upload
        if os.path.isdir(local_path):
            import shutil
            shutil.rmtree(local_path)

def convert_zarr_to_mat(root, filename, file_id):
    data_dict = {}
    interval_doc_ref = db.collection('intervals').where("patientFile", "==", file_id)
    for key in root.array_keys():
        data_dict[key] = root[key][:]
    
    for key, val in root.attrs.items():
        data_dict[key] = val
    intervals = []
    if 'intervals' in data_dict:
        intervals = data_dict['intervals']
    for interval_doc in interval_doc_ref.stream():
        doc = interval_doc.to_dict()
        intervals.append({'intervals': doc['intervals'], 'vital': doc['yaxis']})
    data_dict["intervals"] = intervals
    output_data = {'SData': data_dict}

    output_path = os.path.join(os.getenv('DOWNLOAD_FOLDER', "D:/UoS/COMP6200/firebase/app/tmp"), filename + '.mat')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    savemat(output_path, output_data)
    return output_path

def zip_dir_to_temp(dir_path: str) -> str:
    """Zip a directory to a temp .zip and return the temp file path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()  # we'll write with ZipFile
    with zipfile.ZipFile(tmp.name, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(dir_path):
            for f in files:
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, dir_path)
                zf.write(abs_path, rel_path)
    return tmp.name