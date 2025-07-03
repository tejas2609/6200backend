from firebase_admin import firestore

db = firestore.client()

def get_user(uid: str):
    return db.collection("users").document(uid).get()

def add_user(data: dict):
    return db.collection("users").document(data["uid"]).set(data)

def add_dashboard(data: dict):
    return db.collection("dashboards").add(data)
