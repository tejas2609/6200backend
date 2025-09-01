from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import FileResponse
from app.dependencies.dependencies import get_current_user
from app.services import dataservice, conversion, aggregatorsevice, livedata
from firebase_admin import firestore, storage
import gcsfs
import zarr
import os
import threading
import asyncio
from functools import partial

router = APIRouter()


db = firestore.client()
bucket = storage.bucket()


key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../firebase-key.json"))
fs = gcsfs.GCSFileSystem(project='project-8680797989633263399', token=key_path)

BASE_PATH = 'project-8680797989633263399.firebasestorage.app/'


@router.get("/get-data")
async def get_data(request: Request):
    try:
        params = dict(request.query_params)
        if "patientfile" not in params or "yaxis" not in params:
            raise HTTPException(status_code=400, detail="Missing required query params")

        filename = dataservice.get_file_details(params["patientfile"])
        if not filename:
            raise HTTPException(status_code=404, detail="File not found for the given doc id")

        yaxis = params["yaxis"]
        graph_id = params.get("graphid", "")
        duration = params.get("duration", None)
        filt = params.get("filter", "")

        loop = asyncio.get_running_loop()
        # Run CPU-bound I/O+numpy work in a thread pool
        y_columns, x_columns = await asyncio.gather(
            loop.run_in_executor(None, partial(dataservice.extract_columns1, filename, yaxis, duration, filt, True)),
            loop.run_in_executor(None, partial(dataservice.get_time1, filename, duration, filt)),
        )

        return {
            "x_columns": x_columns,
            "y_columns": y_columns,
            "graph_id": graph_id,
            "status": "success",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.get("/get-data-live")
def get_data_live(request: Request):
    try:
        get_params = dict(request.query_params)
        xaxis = get_params['xaxis']
        yaxis = get_params['yaxis']
        graph_id = get_params['graphid']
        duration = get_params['duration']
        hospital_name = get_params['hospital']
        patientFile = get_params["patientfile"]
        heatMap = get_params.get("allVitalLive", False)
        threading.Thread(target=dataservice.get_live_data, args=(xaxis, yaxis, hospital_name, patientFile, heatMap)).start()
        doc_ref_snapshot = db.collection("live_data").document(hospital_name).collection('patients').document(patientFile).get()
        output_data = {
            "x_xolumns": [],
            "y_columns": []
        }
        if doc_ref_snapshot.exists:
            data = doc_ref_snapshot.to_dict()
            if not heatMap:
                if yaxis in data:
                    x_data = data.get(yaxis, [])
                    for d in x_data:
                        output_data["x_xolumns"].append(d.get("timestamp", 0))
                        output_data["y_columns"].append(d.get("value", 0))
            else:
                obj = {}
                time_obj = []
                print(data)
                for item, values in data.items():
                    obj[item] = [v.get("value", 0) for v in values[-10:]]
                    if not len(time_obj):
                        time_obj = [v.get("timestamp", 0) for v in values[-10:]]
                output_data["x_xolumns"] = time_obj
                output_data["y_columns"] = obj
        return {
            "x_columns": output_data["x_xolumns"],
            "y_columns": output_data["y_columns"],
            "graph_id": graph_id,
            "ref_id": f"live_heatmap_{patientFile}" if heatMap else f"live_{yaxis}",
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
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        else:
            raise HTTPException(status_code=500, detail="Patient data not Found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.get("/get-files")
def get_files(request: Request, 
        current_user: dict = Depends(get_current_user)
    ):
    try:
        get_params = dict(request.query_params)
        hospital_name = get_params.get('hospital')
        files_ref = db.collection('files').order_by("timestamp", direction=firestore.Query.DESCENDING)
        docs = files_ref.get()
        file_list = []
        for doc in docs:
            data = doc.to_dict()
            if data['status'] != 'success':
                continue
            if current_user['role'] == 'user':
                if data.get('selectedUsers'):
                    if current_user['sub'] not in data.get('selectedUsers'):
                        continue
            if not hospital_name and data.get("hospital") != hospital_name:
                continue
            file_name = data.get("storage_path")
            if not file_name:
                continue
            splitted_str = file_name.split("_")
            original_name = '_'.join(splitted_str[2:])
            file_list.append({
                "name": original_name,
                "timestamp": data.get("timestamp"),
                "id": splitted_str[1],
                "doc_id": doc.id,
                "selectedUsers": data.get('selectedUsers'),
                'live': False
            })
            
        live_data_ref = db.collection('live_data').document(hospital_name).collection("patients")
        if len(live_data_ref.get()) == 0:
            return {"files": file_list}
        for doc in live_data_ref.get():
            file_list.append({
                "name": doc.id,
                "url": None,
                "timestamp": None,
                "id": doc.id,
                "doc_id": doc.id,
                'live': True
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

@router.post('/download-file')
async def downloadFile(req: Request, background_tasks: BackgroundTasks):
    try:
        body = await req.json()
        doc_id = body.get('file_id')
        is_live = body.get('live', False)
        
        if is_live:
            converted_resp = conversion.convert_to_zarr_live_data(doc_id, upload = False)
            output_file_path = converted_resp[1]
            output_file_path = conversion.convert_zarr_to_mat(converted_resp[0]['SData'], doc_id, doc_id)
            print(output_file_path)
            if output_file_path and os.path.exists(output_file_path):
                return FileResponse(
                    path=output_file_path,
                    filename=os.path.basename(output_file_path),
                    media_type='application/octet-stream'
                )
            else:
                return HTTPException(status_code=500, detail="Error converting Zarr to MAT file")
        
        doc_name = dataservice.get_file_details(doc_id)
        if doc_name:
            zarr_path = BASE_PATH + doc_name + '/SData'
            mapper = fs.get_mapper(zarr_path)
            root = zarr.open_group(store=mapper, mode='r')
            
            if root:
                output_file_path = conversion.convert_zarr_to_mat(root, doc_name, doc_id)
                if output_file_path and os.path.exists(output_file_path):
                    return FileResponse(
                        path=output_file_path,
                        filename=os.path.basename(output_file_path),
                        media_type='application/octet-stream'
                    )
                else:
                    raise HTTPException(status_code=500, detail="Error converting Zarr to MAT file")
            else:
                raise HTTPException(status_code=404, detail="Zarr data not found")
        else:
            raise HTTPException(status_code=404, detail="File not found")    

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")
    
@router.get('/get-cols-live')
async def gerLiveDataColumns(req: Request):
    try:        
        get_params = dict(req.query_params)
        patientName = get_params['patientName']
        hospital_name = get_params['hospital']
        doc = db.collection("live_data").document(hospital_name).collection("patients").document(patientName).get()
        if doc.exists:
            keys = list(doc.to_dict().keys())
            return {
                'vitals': keys,
                'status': 'success'
            }
        else:
            raise HTTPException(status_code=404, detail="Doc data not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")
    
@router.get('/patients-general-data')
async def generalPatientData(request: Request
    ):
    try:
        get_params = request.query_params
        if 'live' in get_params and get_params['live']:
            return
        patient_file = dataservice.get_file_details(get_params["file_id"])
        data = dataservice.get_patients_general_data(patient_file)
        return{
            'data': data,
            'status': 'success'
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fetching details failed ${e}")

@router.post('/aggregation')
async def performAggregation(request: Request):
    try:
        body = await request.json()
        response = aggregatorsevice.compute_aggregation(body)
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed - {e}")