from fastapi import APIRouter, HTTPException, status
from app.models import DashboardCreate, DashboardInDB
from datetime import datetime
from firebase_admin import firestore
from google.cloud.exceptions import GoogleCloudError

router = APIRouter()
db = firestore.client()

@router.get("/dashboards", response_model=list[DashboardInDB])
async def get_dashboards():
    try:
        dashboards_ref = db.collection("dashboards")
        docs = dashboards_ref.stream()

        dashboards = []
        for doc in docs:
            data = doc.to_dict()
            data["_id"] = doc.id
            dashboards.append(DashboardInDB(**data))

        return dashboards

    except GoogleCloudError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Firestore error: {str(e)}"
        )


@router.post("/save-dashboard", status_code=status.HTTP_201_CREATED)
async def create_dashboard(dashboard: DashboardCreate):
    try:
        now = datetime.utcnow().isoformat()
        doc_data = dashboard.dict()
        doc_data["created_at"] = now
        doc_data["updated_at"] = now

        dashboards_ref = db.collection("dashboards")

        # Check for duplicate name (Firestore doesn't enforce uniqueness)
        query = dashboards_ref.where("name", "==", dashboard.name).stream()
        if any(True for _ in query):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Dashboard with name '{dashboard.name}' already exists."
            )

        doc_ref = dashboards_ref.document()
        doc_ref.set(doc_data)

        doc_data["_id"] = doc_ref.id
        dashboard_in_db = DashboardInDB(**doc_data)

        return {
            "resp_status": "success",
            "dashboard": dashboard_in_db
        }

    except GoogleCloudError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Firestore error: {str(e)}"
        )
