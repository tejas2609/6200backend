from fastapi import APIRouter, HTTPException, Request, status
from app.models import DashboardCreate, DashboardInDB
from datetime import datetime
from firebase_admin import firestore
from google.cloud.exceptions import GoogleCloudError
import uuid
from app.services.dataservice import store_notification

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
        logging_ref = db.collection("logs")
        
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
                log_obj = {
                    'type': 'db',
                    'subtype' : 'edit',
                    'dbId': doc_data["id"],
                    'email': doc_data['email']
                }
                logging_ref.add(log_obj)
                store_notification(dashboard.email, 'user', 'Updated dashboard', 'General Hospital')
                return {
                    "resp_status": "success",
                    "dashboard": dashboard_in_db
                }

        doc_ref = dashboards_ref.document()
        doc_ref.set(doc_data)

        doc_data["id"] = doc_ref.id
        dashboard_in_db = DashboardInDB(**doc_data)

        log_obj = {
            'type': 'db',
            'subtype' : 'save',
            'dbId': doc_data["id"],
            'email': doc_data['email']
        }
        logging_ref.add(log_obj)
        store_notification(dashboard.email, 'user', 'Created dashboard' + dashboard.name, 'General Hospital')
        return {
            "resp_status": "success",
            "dashboard": dashboard_in_db
        }

    except GoogleCloudError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Firestore error: {str(e)}"
        )

@router.delete('/delete-dashboard/{dashboard_id}')
def deleteDashboard(dashboard_id: str):
    try:
        doc_ref = db.collection('dashboards').document(dashboard_id)
        logging_ref = db.collection("logs")
        
        if doc_ref.get().exists:
            store_notification(doc_ref.get().to_dict()['email'], 'user', 'Deleted dashboard', 'General Hospital')
            log_obj = {
                'type': 'db',
                'subtype' : 'delete',
                'dbId': dashboard_id,
                'email': doc_ref.get().to_dict()['email']
            }
            logging_ref.add(log_obj)
            doc_ref.delete()
            return {
                'status': 'success',
                'message': 'Dashboard deleted successfully'
            }
        raise HTTPException(status_code=404, detail='Dashboard not found!')
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Firestore error: {str(e)}"
        )

@router.post('/save-intervals')
async def saveIntervals(request: Request):
    try:
        body = await request.json()
        intervals = body['intervals']        
        interval_doc_ref = db.collection('intervals')

        for interval in intervals:
            yaxis = interval['yaxis']
            patientfile = interval['patientFile']
            interval_value = interval['interval']

            query = interval_doc_ref.where("yaxis", "==", yaxis)\
                .where("patientFile", "==", patientfile) \
                    .limit(1)

            results = query.stream()
            matched_doc = next(results, None)

            if matched_doc:
                doc_ref = interval_doc_ref.document(matched_doc.id)
                doc_ref.update({
                    "intervals": firestore.ArrayUnion([interval_value])
                })
            else:
                interval_doc_ref.add({
                    "yaxis": yaxis,
                    "patientFile": patientfile,
                    "intervals": [interval_value]
                })    
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Firestore error: {str(e)}"
        )