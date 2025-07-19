from fastapi import APIRouter, HTTPException, Request, status
from app.services import dataservice
from firebase_admin import firestore, storage

router = APIRouter()

db = firestore.client()
bucket = storage.bucket()


@router.get("/get-data")
async def get_data(request: Request):
    try:
        get_params = dict(request.query_params)
        print(get_params)
        patient_file = get_params["patientfile"]
        xaxis = get_params['xaxis']
        yaxis = get_params['yaxis']
        graph_id = get_params['id']
        x_columns = None
        y_columns_resp = dataservice.extract_columns(patient_file, yaxis)
        if xaxis == 'Time':
            x_columns = dataservice.get_time(y_columns_resp[1])
        else:
            x_columns = dataservice.extract_columns(patient_file, xaxis)

        return {
            "x_columns": x_columns,
            "y_columns": y_columns_resp[0],
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
            download_status = dataservice.downloadFile(file_name)
            if download_status['status'] == 'success':
                file_path = download_status['file_path']    
                columns = dataservice.extract_col_names(file_path)
                return {
                    "columns": columns,
                    "message": "Column names retrieved successfully"
                }
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
        