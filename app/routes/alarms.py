from fastapi import APIRouter, Depends, HTTPException, Request, status
from app.dependencies.dependencies import get_current_user
from app.services.dataservice import store_notification
from firebase_admin import firestore, storage
import os
import redis

router = APIRouter()


db = firestore.client()
bucket = storage.bucket()
REDIS_URL = os.getenv('REDIS_URL', 'localhost')
r = redis.Redis(host="localhost", port=6379, decode_responses=True)


@router.post('/save-alarm')
async def saveAlarm(request: Request,
        current_user: dict = Depends(get_current_user)
    ):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail='User Unidentified')
        alarm_collection = db.collection('alarms')
        alarm_doc = await request.json()
        
        if 'id' in alarm_doc:
            alarm_query = alarm_collection.document(alarm_doc['id'])
            if alarm_query.get().exists:
                alarm_query.update(alarm_doc)
                return {"message": "Alarm updated", 'status': 'success'}
        
        new_doc_ref = alarm_collection.document()
        alarm_doc['id'] = new_doc_ref.id
        alarm_doc['email'] = current_user['sub']
        new_doc_ref.set(alarm_doc)
        r.delete(f"alarms:{alarm_doc['alarmname']}")
        store_notification(current_user['sub'], 'user', 'Created alarm ' + alarm_doc['alarmname'], current_user.get('hospital', 'General Hospital'))
        return {"message": "Alarm created", 'status' : 'success'}                        
            
    except Exception as e:
        raise HTTPException(status_code=500, detail="Alarm Saving Failed:" + str(e))

@router.get('/get-alarms')
async def getAllAlarms(current_user: dict = Depends(get_current_user)):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail='User Unidentified')
        
        alarm_collection = db.collection('alarms').where('hospital', '==', current_user['hospital'])\
            .where('email', '==', current_user['sub']).stream()
        alarms = []
        for alarm in alarm_collection:
            alarms.append(alarm.to_dict())
        
        return {
            'status': 'success',
            'alarms': alarms
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail="Fetching Failed:" + str(e) + '. Please reload')
    
@router.get('/get-alerts')
async def getAllAlerts(current_user: dict = Depends(get_current_user)):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail='User Unidentified')
        
        alert_collection = db.collection('alerts').where('hospital', '==', current_user['hospital']).stream()
        alerts = []
        for alert in alert_collection:
            doc = alert.to_dict()
            for trigger in doc.get('triggered', []):
                if trigger.get('email') == current_user['sub']:
                    alerts.append(trigger)
            
        return {
            'status': 'success',
            'alerts': alerts
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail="Fetching Failed:" + str(e) + '. Please reload')
    
@router.delete('/delete-alarm/{alarm_id}')
def deleteDashboard(alarm_id: str):
    try:
        doc_ref = db.collection('alarms').document(alarm_id)
        logging_ref = db.collection("logs")
        
        if doc_ref.get().exists:
            store_notification(doc_ref.get().to_dict()['email'], 'user', 'Deleted Alarm', 'General Hospital')
            log_obj = {
                'type': 'db',
                'subtype' : 'delete',
                'dbId': alarm_id,
                'email': doc_ref.get().to_dict()['email']
            }
            logging_ref.add(log_obj)
            doc_ref.delete()
            return {
                'status': 'success',
                'message': 'Alarm deleted successfully'
            }
        raise HTTPException(status_code=404, detail='Alarm not found!')
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Firestore error: {str(e)}"
        )