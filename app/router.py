from fastapi import APIRouter
from app.routes import admin, dashboard, data, auth, alarms, notifications
from app.services import websockets

router = APIRouter()
router.include_router(dashboard.router)
router.include_router(data.router)
router.include_router(auth.router)
router.include_router(admin.router)
router.include_router(alarms.router)
router.include_router(notifications.router)
router.include_router(websockets.router)

