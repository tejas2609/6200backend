from fastapi import APIRouter
from app.routes import admin, dashboard, data, auth

router = APIRouter()
router.include_router(dashboard.router)
router.include_router(data.router)
router.include_router(auth.router)
router.include_router(admin.router)
