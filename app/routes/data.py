from fastapi import APIRouter, HTTPException, Request, status
from app.services import dataservice
from firebase_admin import firestore, storage
import gcsfs
from google.oauth2 import service_account
from app.firebase import firebase_cred
import zarr
import os
import numpy as np

router = APIRouter()


db = firestore.client()
bucket = storage.bucket()

# credentials = service_account.Credentials.from_service_account_file(os.path.abspath(key_path))
# fs = gcsfs.GCSFileSystem(project='project-8680797989633263399', token=credentials) 

key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../firebase-key.json"))
fs = gcsfs.GCSFileSystem(project='project-8680797989633263399', token=key_path)

BASE_PATH = 'project-8680797989633263399.firebasestorage.app/'


@router.get("/get-data")
async def get_data(request: Request):
    try:
        get_params = dict(request.query_params)
        patient_file = dataservice.get_file_details(get_params["patientfile"])
        xaxis = get_params['xaxis']
        yaxis = get_params['yaxis']        
        graph_id = get_params['graphid']
        duration = get_params['duration']
        y_columns = dataservice.extract_columns1(patient_file, yaxis, duration)
        x_columns = dataservice.get_time1(patient_file, duration)
        return {
            "x_columns": x_columns,
            "y_columns": y_columns,
            "graph_id": graph_id,
            "status": "success"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")



@router.get("/get-col-names")
async def get_col_names(request: Request):
    try:
        get_params = dict(request.query_params)
        file_id = get_params['file_id']
        
        if not file_id:
            raise HTTPException(status_code=500, detail="Patient file not received")
        file_name = dataservice.get_file_details(file_id)
        if file_name:      
            zarr_path = BASE_PATH + file_name + '/SData'
            try:
                mapper = fs.get_mapper(zarr_path)
                root = zarr.open_group(store=mapper, mode='r')
                return {
                    "columns" : list(root.keys()),
                    "message": "Column names retrieved successfully"
                }
                # Create interval array inside the group (e.g., shape (50000, 2))
                # intervals = np.array([[0, 1]], dtype="int")

                # Store interval as a proper Zarr dataset
                # root.create_dataset("interval", data=intervals, shape=intervals.shape,chunks=(len(intervals), 2), overwrite=True)
                # interval_data = root["interval"][:]  # Use slicing to load the whole array into memory

                # Print or use the data
                # print("Interval shape:", interval_data.shape)
                # print(interval_data[0][0])
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        else:
            raise HTTPException(status_code=500, detail="Patient data not Found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.get("/get-files")
def get_files():
    try:
        files_ref = db.collection('files').order_by("timestamp", direction=firestore.Query.DESCENDING)
        docs = files_ref.stream()
        
        file_list = []
        for doc in docs:
            data = doc.to_dict()
            file_name = data.get("storage_path")
            if not file_name:
                continue
            splitted_str = file_name.split("_")
            original_name = '_'.join(splitted_str[2:])
            file_list.append({
                "name": original_name,
                "url": data.get("url"),
                "timestamp": data.get("timestamp"),
                "id": splitted_str[1],
                "doc_id": doc.id
            })
        return {"files": file_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.post('/delete-patient-file')
async def deletePatientFile(req: Request):
    try:
        body = await req.json()
        doc_id = body.get('id')

        if not doc_id:
            return {"status": "error", "message": "Missing document ID"}

        doc_name = dataservice.get_file_details(doc_id)
        if doc_name:
            blob = bucket.blob(doc_name)
            if blob.exists():
                blob.delete()
            
            return{
                'status': 'success',
                'message': 'File Deleted'
            }
        else:
            return{
                'status': 'failure',
                'message': 'Issue in deleting the file'
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")
        