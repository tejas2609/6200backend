from fastapi import HTTPException, status
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from firebase_admin import firestore
from datetime import datetime, timedelta
import random
import os

db = firestore.client()

conf = ConnectionConfig(
    MAIL_USERNAME=os.getenv("MAIL_USERNAME", "as1610635@gmail.com"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD", "gywidhojghucmlmy"),
    MAIL_FROM=os.getenv("MAIL_FROM", "as1610635@gmail.com"),
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_STARTTLS=True,          # ✅ New correct field
    MAIL_SSL_TLS=False,          # ✅ New correct field
    USE_CREDENTIALS=True
)
def generate_otp() -> str:
    return str(random.randint(100000, 999999))

async def send_otp_email(email: str, otp: str):
    message = MessageSchema(
        subject="Your OTP Code",
        recipients=[email],
        body=f"Your OTP code is: {otp}",
        subtype="plain"
    )
    fm = FastMail(conf)
    await fm.send_message(message)

def store_otp(email: str, otp: str):
    db.collection("otp").document(email).set({
        "otp": otp,
        "created_at": datetime.utcnow().isoformat()
    })

def verify_otp_code(email: str, otp: str, expiry_minutes: int = 10):
    doc = db.collection("otp").document(email).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="OTP not found")

    data = doc.to_dict()

    created_time = datetime.fromisoformat(data["created_at"])
    if datetime.utcnow() - created_time > timedelta(minutes=expiry_minutes):
        raise HTTPException(status_code=400, detail="OTP expired")

    if data["otp"] != otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    return True
