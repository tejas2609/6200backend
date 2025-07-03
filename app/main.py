from app import firebase  # triggers Firebase initialization
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app import router
from dotenv import load_dotenv


app = FastAPI()

load_dotenv() 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router.router)
