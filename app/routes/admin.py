from datetime import datetime
import os
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Depends, Request, UploadFile, WebSocket, WebSocketDisconnect
from firebase_admin import firestore, storage
from pydantic import BaseModel
import uuid
import shutil

from app.services.conversion import mat_file_conversion

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
                "timestamp": datetime.utcnow()
            })

            background_tasks.add_task(upload_with_socket_progress, doc_ref, filename, zarr_path, unique_id)

            return {"upload_id": unique_id, "status": "queued"}
            
    except Exception as e:
        return {"error": str(e)}
    finally:
        if os.path.exists(temp_mat_path):
            os.remove(temp_mat_path)

async def send_progress(upload_id: str, progress: float):
    ws = connections.get(upload_id)
    if ws:
        try:
            await ws.send_json({"progress": progress})
        except:
            pass

def upload_with_socket_progress(doc_ref, filename, local_path, upload_id):
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
        # Remove local folder after upload
        if os.path.isdir(local_path):
            import shutil
            shutil.rmtree(local_path)

