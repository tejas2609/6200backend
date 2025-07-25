from datetime import datetime
import hashlib
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Request
from firebase_admin import firestore
from pydantic import BaseModel
from app.utils.utils import hash_password, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from app.dependencies.dependencies import get_current_user
from app.routes.otp import generate_otp, store_otp, send_otp_email, verify_otp_code
from google.cloud.firestore_v1 import FieldFilter


router = APIRouter()

db = firestore.client()

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()
class LoginRequest(BaseModel):
    email: str
    password: str
    user_role: str
    hospital: Optional[str]
class RegisterRequest(BaseModel):
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    password: Optional[str]
    user_role: Optional[str]
    hospital: Optional[str]
    contact: Optional[str]
class TokenResponse(BaseModel):
    status: str = 'success'
    access_token: str
    token_type: str = "bearer"
    expires_in: int
class OTPVerifyRequest(BaseModel):
    email: str
    otp: str
class UnverifiedUsers(BaseModel):
    hospital: str

class ResetPassword(BaseModel):
    email: str
    new_pass: str
    user_role: str
    old: Optional[str]
    hospital: Optional[str]
    

@router.post("/register")
async def register(req: RegisterRequest):
    try:
        if req.user_role not in ("admin", "user"):
            raise HTTPException(status_code=400, detail="user_role must be 'admin' or 'user'")

        collection = db.collection(req.user_role)
        docs = collection.where("email", "==", req.email).limit(1).stream()
        existing_user_doc = next(docs, None)

        # Case 1: If user already exists
        if existing_user_doc:
            user = existing_user_doc.to_dict()

            if not user.get("verified", False):
                # ✅ Resend OTP instead of error
                otp = generate_otp()
                store_otp(req.email, otp)
                await send_otp_email(req.email, otp)

                return {
                    "status": "success",
                    "message": "Unverified account found. OTP has been resent."
                }
            else:
                raise HTTPException(status_code=400, detail="Email is already registered and verified.")

        # Case 2: Fresh registration
        now = datetime.utcnow().isoformat()
        user_data = {
            "first_name": req.first_name,
            "last_name": req.last_name,
            "email": req.email,
            "password": hash_password(req.password),
            "hospital": req.hospital,
            "contact": req.contact,
            "verified": False,
            "created_at": now,
            "accepted": False
        }
        if req.user_role == 'user':
            notification_data = {
                "hospital": req.hospital,
                "email": req.email,
                "read": False,
                "created_at": now,
                "action": "user_add"
            }            
            notificaton_collection = db.collection("notifications")
            notificaton_collection.add(notification_data)

        collection.add(user_data)

        # Send OTP
        otp = generate_otp()
        store_otp(req.email, otp)
        await send_otp_email(req.email, otp)

        return {
            "status": "success",
            "message": f"{req.user_role.capitalize()} registered. OTP sent to email."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@router.post("/login", response_model=TokenResponse)
async def login(req: LoginRequest):
    try:
        if req.user_role not in ("admin", "user"):
            raise HTTPException(status_code=400, detail="user_role must be 'admin' or 'user'")

        collection = db.collection(req.user_role)
        docs = collection.where("email", "==", req.email).limit(1).get()
        user = docs[0].to_dict()
        
        if not user:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        if not user['accepted'] and req.user_role == 'user':
            raise HTTPException(status_code=401, detail="You have not been approved by the administrator yet.")
        
        if user["password"] != hash_password(req.password):
            raise HTTPException(status_code=401, detail="Invalid email or password")

        if user["hospital"] != req.hospital:
            raise HTTPException(status_code=401, detail="You aren't registered for ${req.hospital}")

        token_data = {"sub": req.email, "role": req.user_role}
        if 'hospital' in user:
            token_data['hospital'] = user['hospital']
        access_token = create_access_token(token_data)

        return TokenResponse(
            access_token=access_token,
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 120
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/profile")
async def get_profile(current_user: dict = Depends(get_current_user)):
    return {"email": current_user["sub"], "role": current_user["role"], "hospital": current_user['hospital']}

@router.post("/verify-otp")
async def verify_otp(req: OTPVerifyRequest):
    verify_otp_code(req.email, req.otp)

    user_ref = db.collection("user").where("email", "==", req.email).limit(1).stream()
    user_doc = next(user_ref, None)
    if user_doc:
        db.collection("user").document(user_doc.id).update({"verified": True})

    return {"status": "success", "message": "OTP verified successfully"}

@router.post('/forgot-pass')
async def forgotPass(req: Request):
    try:
        body = await req.json()
        user_role = body.get('user_role')
        email = body.get('email')
        collection = db.collection(user_role)
        docs = collection.where("email", "==", email).limit(1).get()
        user = docs[0].to_dict()
        
        if not user:
            raise HTTPException(status_code=401, detail="Invalid Email, please provide the correct email.")

        otp = generate_otp()
        store_otp(email, otp)
        await send_otp_email(email, otp)
        
        return {
            'status': 'success',
            'message': 'Otp sent successfully'
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/reset-pass")
async def resetPass(req: Request):
    try:
        body = await req.json()
        if body.get("user_role") not in ['admin', 'user']:
            raise HTTPException(status_code=401, detail="Admin or user role required")

        collection = db.collection(body.get("user_role"))
        docs = collection.where("email", "==", body.get("email")).limit(1).get()
        user = docs[0].to_dict()
        
        if not user:
            raise HTTPException(status_code=401, detail="Incorrect email provided")

        if user['password'] == hash_password(body.get("new_pass")):
            raise HTTPException(status_code=401, detail="Old password entered. Please enter password different than last one.")

        doc_ref = docs[0].reference
        doc_ref.update({
            "password": hash_password(body.get("new_pass"))
        })
        
        return{
            "status": "success",
            "message": "Password reset successfully."
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/get-profile-unverified")
async def unverified_profiles(req: UnverifiedUsers):
    try:
        collection = db.collection('user')
        docs = (
            collection
            .where("accepted", "==", False)
            .where("verified", "==", True)
            .where("hospital", "==", req.hospital)
            .get()
        )
        # print(docs)
        results = []
        for d in docs:
            doc = d.to_dict()
            if 'barred' in doc:
                if not doc['barred']:
                    results.append(doc)
            else:
                results.append(doc)
                
        return {
            "users": results,
            "status": "success"
        }
              
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))