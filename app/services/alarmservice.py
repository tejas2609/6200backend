# app/services/alarmservice.py
import json
import asyncio
import threading
import logging
import os
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from firebase_admin import firestore
import redis
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
import queue

from app.services.alert_engine import AlertEngine

logging.basicConfig(level=logging.INFO)

# ---------- Env & clients ----------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
POINTS_TOPIC = os.getenv("POINTS_TOPIC", "vitals_points")     # one point per tick
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "triggered_alerts")  # outputs

db = firestore.client()
REDIS_URL = os.getenv("REDIS_URL", "localhost")
r = redis.Redis(host=REDIS_URL, port=6379, decode_responses=True)

# ---------- Email queue ----------
alert_queue = queue.Queue()
conf = ConnectionConfig(
    MAIL_USERNAME=os.getenv("MAIL_USERNAME", "as1610635@gmail.com"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD", "gywidhojghucmlmy"),
    MAIL_FROM=os.getenv("MAIL_FROM", "as1610635@gmail.com"),
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True
)

async def send_email_alert(alert):
    try:
        message = MessageSchema(
            subject=f"Alert: Condition met for {alert['patient']}",
            recipients=[alert["email"]],
            body=(
                f"Condition Met for {alert['patient']}!\n\n"
                f"Alarm '{alert['alarmname']}' / Condition '{alert['conditionname']}'\n"
                f"{alert['vital']} = {alert['value']} (rule: {alert['condition']} {alert['threshold']})\n"
                f"Timestamp: {alert['timestamp']}"
            ),
            subtype="plain"
        )
        fm = FastMail(conf)
        await fm.send_message(message)
        logging.info(f"Email sent to {alert['email']} for {alert['patient']}")
    except Exception as e:
        logging.error(f"Email send failed: {e}")
        db.collection('failed_alert_emails').add(alert)

def email_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        alert = alert_queue.get()
        if alert is None:
            break
        loop.run_until_complete(send_email_alert(alert))
        alert_queue.task_done()

# ---------- Alarm loading (exact patient match only) ----------
def load_alarms(patient_id: str):
    redis_key = f"alarms:{patient_id}"
    cached = r.get(redis_key)
    if cached:
        return json.loads(cached)

    docs = db.collection("alarms").where("patientFile", "==", patient_id).stream()
    alarms = [doc.to_dict() for doc in docs]
    r.set(redis_key, json.dumps(alarms), ex=300)
    return alarms

# Cache engines per patient
_ENGINES: dict[str, AlertEngine] = {}

def get_engine(patient_id: str) -> AlertEngine:
    eng = _ENGINES.get(patient_id)
    if eng:
        return eng
    alarms_cfg = load_alarms(patient_id)
    eng = AlertEngine(alarms_cfg)
    _ENGINES[patient_id] = eng
    return eng

# ---------- Main consumer ----------
def start_alarm_evaluator():
    threading.Thread(target=email_worker, daemon=True).start()
    logging.info("Email worker thread started.")

    consumer = KafkaConsumer(
        POINTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="alarm_evaluator_points",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    alert_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logging.info(f"Listening to Kafka topic '{POINTS_TOPIC}' for realtime points...")

    for msg in consumer:
        try:
            payload = msg.value
            hospital = payload.get("hospital")
            patient = payload.get("patient")
            point = payload.get("data") or {}   # dict like {"CO2": 21.9, "HeartRate": 120, ...}
            ts = payload.get("timestamp") or int(datetime.utcnow().timestamp() * 1000)

            if not hospital or not patient or not point:
                logging.warning(f"Invalid message skipped: {payload}")
                continue

            # logging.info(f"[POINT] patient={patient} ts={ts} keys={list(point.keys())} sample={point}")

            eng = get_engine(patient)
            if not eng:
                continue

            fired = eng.evaluate_point(patient, ts, point)
            if not fired:
                continue
            # Build alert doc(s)
            alarms_cfg = load_alarms(patient)  # from Redis, cheap
            triggered = []
            for alarm_id, data in fired:
                # find the same alarm config to fetch email + condition details
                alarm_cfg = next((a for a in alarms_cfg if
                                  (a.get("alarmId") or a.get("id") or a.get("alarmname")) == alarm_id), None)
                if not alarm_cfg:
                    continue
                email = alarm_cfg.get("email")
                cond = (alarm_cfg.get("conditions", [{}])[0]) if alarm_cfg else {}
                vital = cond.get("conditionVital", "CO2")
                val = point.get(vital)
                condition_op = cond.get("conditionOp", "lteq")
                threshold = cond.get("conditionValue", 0)

                alert_item = {
                    "vital": vital,
                    "value": val,
                    "timestamp": ts,
                    "condition": condition_op,
                    "threshold": threshold,
                    "email": email,
                    "patient": patient,
                    "alarmname": data["alarmname"],
                    "conditionname": cond.get("conditionName", "condition"),
                }
                triggered.append(alert_item)

                if email:
                    alert_queue.put(alert_item)

            if triggered:
                alert_doc = {
                    "hospital": hospital,
                    "patient": patient,
                    "detected_at": datetime.utcnow().isoformat(),
                    "triggered": triggered
                }
                db.collection("alerts").add(alert_doc)
                alert_producer.send(ALERTS_TOPIC, alert_doc)
                logging.info(f"[ALERT] {patient}: {triggered}")

        except Exception as e:
            logging.error(f"Error processing Kafka message: {e}")
