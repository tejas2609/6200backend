import threading
from dotenv import load_dotenv
import os
import datetime

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

from app import firebase  # triggers Firebase initialization
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app import router
import shutil
from apscheduler.schedulers.background import BackgroundScheduler

from app.services.alarmservice import start_alarm_evaluator
from app.services.kafkapublisher import start_producing
from app.services.livedata import start_storage_writer

# from app.services.livedata import generate_Data1

DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "./firebase/app/tmp")

app = FastAPI()
app.include_router(router.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200",
        "http://127.0.0.1:4200",
        "file://"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": "Z",
        "download_folder": DOWNLOAD_FOLDER,
    }

scheduler = BackgroundScheduler()

def scheduled_job():
    folder_path = os.getenv('DOWNLOAD_FOLDER',"D:/UoS/COMP6200/firebase/app/tmp")
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
            print(f"Deleted folder: {folder_path}")
        except Exception as e:
            print(f"Error deleting folder: {e}")
    else:
        print(f"Folder does not exist: {folder_path}")


@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(scheduled_job, 'cron', minute=0)  # Every hour at :00
    scheduler.start()
    print("APScheduler started.")

@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()
    print("APScheduler stopped.")
    
@app.on_event('startup')
def start_kafkas_services():
    x = 0
    threading.Thread(target=start_producing, daemon=True).start()
    threading.Thread(target=start_alarm_evaluator, daemon=True).start()
    threading.Thread(target=start_storage_writer, daemon=True).start()