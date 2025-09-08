import numpy as np
from firebase_admin import firestore, storage
import os
import redis
from app.services import dataservice
import time
from google.cloud import firestore as gfirestore

db = firestore.client()
bucket = storage.bucket()

REDIS_URL = os.getenv('REDIS_URL')
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def redis_keys(hospital: str, patient: str, email: str, vital: str, aggregator: str):
    base = f"{hospital}:{patient}:{email}:{vital}:{aggregator}"
    return (
        f"aggval:{base}", 
        f"aggmeta:{base}", 
    )

def aggregate(values: np.ndarray, aggregator: str) -> float:
    if aggregator == "max":
        try:
            return float(np.nanmax(values))
        except ValueError:
            return float("nan")
    if aggregator == "min":
        try:
            return float(np.nanmin(values))
        except ValueError:
            return float("nan")
    if aggregator == "mean":
        try:
            return float(np.nanmean(values))
        except ValueError:
            return float("nan")
    if aggregator == "median":
        try:
            return float(np.nanmedian(values))
        except ValueError:
            return float("nan")
    if aggregator == "sd":
        try:
            return float(np.nanstd(values, ddof=0))
        except ValueError:
            return float("nan")

def compute_aggregation(payload):
    val_key, meta_key = redis_keys(payload['hospitalname'], payload['patientFile'], payload['email'], payload['vital'], payload['aggregator'])
    
    meta = r.hgetall(meta_key) or {}
    cached_value = r.get(val_key)

    filename = dataservice.get_file_details(payload['patientFile'])
    values = dataservice.extract_columns1(filename, payload['vital'], None, '', False)
    values = np.asarray(values, dtype=np.float32)
    
    if cached_value is not None:
        return {
            "vital": payload['vital'],
            "value": float(cached_value),
            "aggregator" : payload['aggregator'],
            "status": "success"
        }
    
    result = aggregate(values, payload['aggregator'])
    query = db.collection('dashboards').document(payload['dashboardid'])
    if query.get().exists:
        query.update({
            "aggregation" : gfirestore.ArrayUnion([{'vital' : payload['vital'], 'value': result, 'aggregator': payload['aggregator']}])
        })
    
    ts = str(int(time.time()))
    pipe = r.pipeline()
    pipe.set(val_key, result, ex=3600)
    pipe.hset(meta_key, mapping={"updated_at": ts})
    pipe.execute()
    
    return {
        "vital": payload['vital'],
        "value": result,
        "aggregator" : payload['aggregator'],
        "status": "success"
    }