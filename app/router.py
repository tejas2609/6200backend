from fastapi import APIRouter
from app.routes import user, dashboard, data, auth

router = APIRouter()
router.include_router(dashboard.router)
router.include_router(data.router),
router.include_router(auth.router)
