import threading
import time
from app import firebase  # triggers Firebase initialization
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app import router
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import os
import shutil
from apscheduler.schedulers.background import BackgroundScheduler

from app.services.livedata import flush_to_firestore, generate_data


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

# Create the scheduler
scheduler = BackgroundScheduler()

# Define your scheduled job (e.g., delete a folder or log hello)
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
def start_live_data():
    x = 0
    # threading.Thread(target=generate_data, daemon=True).start()
    # threading.Thread(target=flush_to_firestore, daemon=True).start()
    # while True:
    #     time.sleep(60)

@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(scheduled_job, 'cron', minute=0)  # Every hour at :00
    scheduler.start()
    print("APScheduler started.")

@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()
    print("APScheduler stopped.")