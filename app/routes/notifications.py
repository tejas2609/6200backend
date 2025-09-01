from fastapi import APIRouter, Depends, HTTPException, Request
from firebase_admin import firestore

from app.dependencies.dependencies import get_current_user

router = APIRouter()
db = firestore.client()

@router.get('/all-notifications')
async def get_all_notifications(current_user: dict = Depends(get_current_user)):
    try:
        email = current_user['sub']
        hospital = current_user['hospital']
        notification_ref = db.collection('notifications').where('hospital', '==', hospital)\
            .where('email', '==', email).stream()
        user_notifications = ['New File uploaded', 'Created alarm', 'Created dashboard', 'Updated dashboard', 'Deleted dashboard']
        admin_notifications = ['New User registered', 'User verified', 'User deleted', 'User LoggedIn', 'Password Reset']
        notifications = []
        for n in notification_ref:
            doc = n.to_dict()
            if current_user['role'] == 'user' and doc['type'] in user_notifications:
                notifications.append(doc)
            elif current_user['role'] == 'admin' and doc['type'] in admin_notifications:
                notifications.append(doc)
        
        return {
            'notifications': notifications,
        }
        
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))

@router.post('/read-notifications')
async def read_notifications(req: Request):
    try:
        body = await req.body
        notification_ids = body.get('notifications')
        notification_ref = db.collection('notifications')
        
        for id in notification_ids:
            doc_ref = notification_ref.document(id)
            doc_ref.update({"read": True})
    
        return 
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))
        