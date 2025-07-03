from datetime import datetime
import hashlib
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from firebase_admin import firestore
from pydantic import BaseModel
from app.utils.utils import hash_password, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from app.dependencies.dependencies import get_current_user
from app.routes.otp import generate_otp, store_otp, send_otp_email, verify_otp_code

router = APIRouter()

db = firestore.client()

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

class LoginRequest(BaseModel):
    email: str
    password: str
    user_role: str  # Only used to choose collection

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
            "created_at": now
        }

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
        docs = collection.where("email", "==", req.email).limit(1).stream()
        doc = next(docs, None)
        if not doc:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        user = doc.to_dict()
        if user["password"] != hash_password(req.password):
            raise HTTPException(status_code=401, detail="Invalid email or password")

        token_data = {"sub": req.email, "role": req.user_role}
        access_token = create_access_token(token_data)

        return TokenResponse(
            access_token=access_token,
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/profile")
async def get_profile(current_user: dict = Depends(get_current_user)):
    return {"email": current_user["sub"], "role": current_user["role"]}

@router.post("/verify-otp")
async def verify_otp(req: OTPVerifyRequest):
    verify_otp_code(req.email, req.otp)

    user_ref = db.collection("user").where("email", "==", req.email).limit(1).stream()
    user_doc = next(user_ref, None)
    if user_doc:
        db.collection("user").document(user_doc.id).update({"verified": True})

    return {"status": "success", "message": "OTP verified successfully"}