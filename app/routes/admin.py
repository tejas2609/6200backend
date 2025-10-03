from datetime import datetime
import os
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Depends, Request, UploadFile, WebSocket, WebSocketDisconnect
from firebase_admin import firestore, storage
from pydantic import BaseModel
import uuid
from google.cloud import firestore as gfirestore
from app.dependencies.dependencies import get_current_user
from app.routes.auth import UnverifiedUsers
from app.services.conversion import mat_file_conversion
from app.services.websockets import send_progress
from app.services.dataservice import store_notification

router = APIRouter()

db = firestore.client()
bucket = storage.bucket()

class userActionByAdminModel(BaseModel):
    email: str
    hospital: str
    action: str

@router.post('/user-action-admin')
async def UserActionByAdmin(req: userActionByAdminModel):
    try:
        collection = db.collection('user')
        docs = collection.where("email", "==", req.email).limit(1).stream()
        existing_user_doc = next(docs, None)
        
        if not existing_user_doc:
            raise HTTPException(status_code=400, detail="User not found")

        if req.action == 'accept':
            db.collection("user").document(existing_user_doc.id).update({"accepted": True, "barred": False})
        if req.action == 'reject':
            db.collection("user").document(existing_user_doc.id).update({"accepted": False, "barred": True})

        return {
            "status": "success",
            "message": "User Accepted Successfully"
        }
    except Exception as e:
        raise HTTPException(stcatus_code=500, detail=f"Registration failed: {str(e)}")

@router.post("/barr-user-action")
async def barr_user(req: Request):
    try:
        body = await req.json()
        collection = db.collection('user')
        docs = collection.where("email", "==", body['email']).limit(1).stream()
        existing_user_doc = next(docs, None)
        
        if not existing_user_doc:
            raise HTTPException(status_code=400, detail="User not found")

        db.collection("user").document(existing_user_doc.id).update({"barred": body['barred']})

        return {
            "status": "success",
            "message": "User Updated Successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Operation failed: {str(e)}")

@router.post("/upload-patient-file")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user: str = Form(...),
    hospital: str = Form(...)
):
    folder_path = os.getenv('DOWNLOAD_FOLDER',"D:/UoS/COMP6200/firebase/app/tmp")
    os.makedirs(folder_path, exist_ok=True)
    original_filename = file.filename
    temp_mat_path = os.path.join(folder_path, original_filename)

    with open(temp_mat_path, "wb") as f:
        f.write(await file.read())
    try:
        zarr_details = mat_file_conversion(temp_mat_path, folder_path)
        zarr_path = ''
        zarr_filename = ''
        if zarr_details:
            zarr_path = zarr_details[0]
            zarr_filename = zarr_details[1]
        if zarr_path:
            unique_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            filename = f"{timestamp}_{unique_id}_{zarr_filename}"
            
            # Insert placeholder doc
            doc_ref = db.collection("files").document(unique_id)
            doc_ref.set({
                "status": "in_progress",
                "progress": 0,
                "filename": filename,
                "user": user,
                "hospital": hospital,
                "timestamp": datetime.utcnow(),
                "selectedUsers": []
            })

            background_tasks.add_task(upload_with_socket_progress, doc_ref, filename, zarr_path, unique_id)

            return {"upload_id": unique_id, "status": "queued"}
            
    except Exception as e:
        return {"error": str(e)}
    finally:
        if os.path.exists(temp_mat_path):
            os.remove(temp_mat_path)

@router.post("/get-profile-unverified")
async def unverified_profiles(req: UnverifiedUsers):
    try:
        collection = db.collection('user')
        docs = (
            collection
            .where("accepted", "==", False)
            .where("verified", "==", True)
            .where("hospital", "array_contains", req.hospital)
            .get()
        )
        # print(docs)
        results = []
        for d in docs:
            doc = d.to_dict()
            if 'barred' in doc:
                if not doc['barred']:
                    results.append(doc)
            else:
                results.append(doc)
                
        return {
            "users": results,
            "status": "success"
        }
              
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/get-verified-users")
async def unverified_profiles(req: Request):
    try:
        body = req.query_params
        collection = db.collection('user')
        docs = (
            collection
            .where("accepted", "==", True)
            .where("verified", "==", True)
            .where("hospital", "array_contains", body['hospital'])
            .get()
        )
        results = []
        for d in docs:
            doc = d.to_dict()
            obj = {
                'email' : doc['email'],
                'name' : doc['first_name'] + ' ' + doc['last_name'],
                'barred' : doc['barred'] if 'barred' in doc else False,
                'created_at': int(datetime.fromisoformat(doc['created_at']).timestamp())
            }   
            results.append(obj) 
        return {
            "users": results,
            "status": "success"
        }
              
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/update-access')
async def updateAccessOfFiles(req: Request):
    try:
        body = await req.json()
        access_structure = body.get('access_structure')
        
        for struct in access_structure:
            doc_ref = db.collection('files').document(struct['id'])
            if doc_ref.get().exists:
                doc_ref.update({
                    'selectedUsers': struct['selectedUsers']
                })
        return {
            'status': 'success',
            'message': 'Access Updated'
        }    
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get('/get-file-requests')
async def getRequestAccess(current_user: dict = Depends(get_current_user)):
    try:
        doc_ref = db.collection('file-requests').where('hospital', '==', current_user['hospital']).get()
        files = []
        
        for doc in doc_ref:
            doc_dict = doc.to_dict()
            files.append(doc_dict)
        
        return {
            'status' : 'success',
            'files' : files
        }
        
    except Exception as e:
        return HTTPException(status_code=500, detail="Error fetching requests")

@router.post('/request-access')
async def requestAccess(req: Request, current_user: dict = Depends(get_current_user)):
    try:
        body = await req.json()
        user_email = body.get('email')
        patientFileName = body.get('patientFileName')
        patientFileId = body.get('patientFileId')
        hospital = body.get('hospital')
        
        doc_ref = db.collection('file-requests').document()
        doc_obj = {
            'email': user_email,
            'patientFileName': patientFileName,
            'patientFileId': patientFileId,
            'hospital': hospital,
            'granted': False
        }
        doc_ref.set(doc_obj)
        
        return {
            'status' : 'success',
            'message': 'Access Requested'
        }

    except Exception as e:
        return HTTPException(status_code=500, detail='Error granting access')
    
@router.post('/grant-access')
async def grantAccess(req: Request, current_user: dict = Depends(get_current_user)):
    try:
        body = await req.json()
        user_email = body.get('email')
        patientFile = body.get('patientFile')
        
        doc_ref = db.collection('files').document(patientFile)
        if doc_ref.get():
            doc_ref.update({
                "selectedUsers": firestore.ArrayUnion([user_email])
            })
        
        docs = db.collection('file-requests').where('email', '==', user_email) .where('patientFileId', '==', patientFile).limit(1).get()

        if docs: 
            for doc in docs:
                doc_ref = doc.reference
                doc_ref.update({
                    'granted': True
                })
        else:
            return HTTPException(status_code=404, detail='File Not Found')
    
        return {
            'status' : 'success',
            'message': 'Access Granted'
        }
        
    except Exception as e:
        return HTTPException(status_code=500, detail='Error granting access' + str(e))

def upload_with_socket_progress(doc_ref, filename, local_path, upload_id):
    import asyncio

    try:
        total_size = 0
        file_list = []

        for root, _, files in os.walk(local_path):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, local_path).replace("\\", "/")
                blob_path = f"{filename}/{rel_path}"
                file_list.append((full_path, blob_path))
                total_size += os.path.getsize(full_path)

        uploaded = 0
        chunk_size = 1024 * 1024

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
        store_notification(doc_ref.get().to_dict()['user'], 'user', 'New File uploaded', doc_ref.get().to_dict()['hospital'])
        asyncio.run(send_progress(upload_id, 100))

    except Exception as e:
        doc_ref.update({"status": "failed", "error": str(e)})
        asyncio.run(send_progress(upload_id, -1))

    finally:
        # Remove local folder after upload
        if os.path.isdir(local_path):
            import shutil
            shutil.rmtree(local_path)