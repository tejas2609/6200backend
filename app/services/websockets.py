# import redis
# r = redis.Redis(host='localhost', port=6379, decode_responses=True)
# r.flushdb() 
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()

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

async def send_live_update(upload_id: str, data: dict):
    ws = connections.get(upload_id)
    if ws:
        try:
            await ws.send_json(data)
        except:
            print(f"WebSocket send failed for {upload_id}")

async def send_progress(upload_id: str, progress: float):
    ws = connections.get(upload_id)
    if ws:
        try:
            await ws.send_json({"progress": progress})
        except:
            pass