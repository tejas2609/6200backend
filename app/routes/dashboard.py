from fastapi import APIRouter, HTTPException, status
from app.models import DashboardCreate, DashboardInDB
from datetime import datetime
from firebase_admin import firestore
from google.cloud.exceptions import GoogleCloudError
import uuid

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
            data["id"] = doc.id
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
        
        graphs = doc_data['graphs']
        for i in range(0, len(graphs)):
            if 'id' not in graphs[i] or not graphs[i]['id']:
                graphs[i]['id'] = str(uuid.uuid4())
        doc_data["graphs"] = graphs

        if doc_data["id"]:
            query = dashboards_ref.document(doc_data["id"])
            if query.get().exists:
                query.update(doc_data)
                dashboard_in_db = DashboardInDB(**doc_data)
                
                return {
                    "resp_status": "success",
                    "dashboard": dashboard_in_db
                }

        doc_ref = dashboards_ref.document()
        doc_ref.set(doc_data)

        doc_data["id"] = doc_ref.id
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
