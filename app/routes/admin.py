from datetime import datetime
import hashlib
import os
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Depends, Request, UploadFile, WebSocket, WebSocketDisconnect
from firebase_admin import firestore, storage
from pydantic import BaseModel
import uuid

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
    unique_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}_{unique_id}_{file.filename}"
    local_path = f"{filename}"

    with open(local_path, "wb") as f:
        f.write(await file.read())

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

    background_tasks.add_task(upload_with_socket_progress, doc_ref, filename, local_path, unique_id)

    return {"upload_id": unique_id, "status": "queued"}

async def send_progress(upload_id: str, progress: float):
    ws = connections.get(upload_id)
    if ws:
        try:
            await ws.send_json({"progress": progress})
        except:
            pass

def upload_with_socket_progress(doc_ref, filename, local_path, upload_id):
    import asyncio
    blob = bucket.blob(f"{filename}")
    file_size = os.path.getsize(local_path)
    chunk_size = 1024 * 1024
    uploaded = 0

    try:
        with open(local_path, "rb") as f:
            writer = blob.open("wb", content_type="application/octet-stream")

            while chunk := f.read(chunk_size):
                writer.write(chunk)
                uploaded += len(chunk)
                progress = round((uploaded / file_size) * 100, 2)
                doc_ref.update({"progress": progress})
                asyncio.run(send_progress(upload_id, progress))

            writer.close()

        blob.make_public()
        doc_ref.update({
            "status": "success",
            "url": blob.public_url,
            "storage_path": blob.name
        })
        asyncio.run(send_progress(upload_id, 100))

    except Exception as e:
        doc_ref.update({"status": "failed", "error": str(e)})
        asyncio.run(send_progress(upload_id, -1))

    finally:
        os.remove(local_path)