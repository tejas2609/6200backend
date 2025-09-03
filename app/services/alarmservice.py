import json
import asyncio
import threading
from datetime import datetime
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from kafka import KafkaConsumer, KafkaProducer
from firebase_admin import firestore
import redis
import os
import queue
import logging

logging.basicConfig(level=logging.INFO)

db = firestore.client()
REDIS_URL = os.getenv('REDIS_URL', 'localhost')
r = redis.Redis(host=REDIS_URL, port=6379, decode_responses=True)

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "vitals_data"
KAFKA_ALERTS = "triggered_alerts"

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

alert_queue = queue.Queue()

async def send_email_alert(alert):
    vital = alert['vital']
    value = alert['value']
    alarmname = alert['alarmname']
    conditionname = alert['conditionname']
    patientName = alert['patientName']
    email = alert['email']

    content = f"""Condition Met for {patientName}!!!

Condition '{conditionname}' you have set for alarm '{alarmname}'
Current Value of {vital} = {value}
"""

    try:
        message = MessageSchema(
            subject=f"Alert: Condition met for {patientName}",
            recipients=[email],
            body=content,
            subtype="plain"
        )
        fm = FastMail(conf)
        await fm.send_message(message)
        logging.info(f"Email sent to {email} for patient {patientName}, vital {vital}")
    except Exception as e:
        logging.error(f"Failed to send email to {email}: {e}")
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


def load_alarms(patient_id):
    redis_key = f"alarms:{patient_id}"
    cached = r.get(redis_key)
    if cached:
        return json.loads(cached)
    docs = db.collection("alarms").where("patientFile", "==", patient_id).stream()
    alarms = [doc.to_dict() for doc in docs]
    r.set(redis_key, json.dumps(alarms), ex=300)
    return alarms


def evaluate_alarms(batch, alarms):
    triggered = []
    for alarm in alarms:
        conditions = alarm.get('conditions', [])
        email = alarm.get('email')
        patientName = alarm.get('patientFile')
        alarmname = alarm.get('alarmname')
        for cond in conditions:
            vital = cond.get('conditionVital')
            threshold = cond.get('conditionValue')
            condOperator = cond.get('conditionOp')
            conditionName = cond.get('conditionName')
            for entry in batch:
                if vital not in entry:
                    continue
                val = entry[vital]
                if ((condOperator == "gteq" and val >= threshold) or
                    (condOperator == "lteq" and val <= threshold) or
                    (condOperator == "eq" and val == threshold) or
                    (condOperator == "ne" and val != threshold)):
                    
                    alert = {
                        "vital": vital,
                        "value": val,
                        "timestamp": entry.get("timestamp"),
                        "condition": condOperator,
                        "threshold": threshold,
                        "message": f"{vital} met your condition {{{condOperator}, {threshold}}}",
                        "email": email,
                        "patientName": patientName,
                        "alarmname": alarmname,
                        "conditionname": conditionName
                    }
                    triggered.append(alert)
                    alert_queue.put(alert) 
    return triggered


def start_alarm_evaluator():
    threading.Thread(target=email_worker, daemon=True).start()
    logging.info("Email worker thread started.")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="alarm_evaluator"
        )
        alert_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logging.info(f"Listening to Kafka topic '{KAFKA_TOPIC}' for vitals...")

        for msg in consumer:
            try:
                payload = msg.value
                hospital = payload.get("hospital")
                patient = payload.get("patient")
                batch = payload.get("data", [])

                if not hospital or not patient or not batch:
                    logging.warning(f"Invalid Kafka message skipped: {payload}")
                    continue

                alarms = load_alarms(patient)
                triggered = evaluate_alarms(batch, alarms)
                
                if triggered:
                    alert_doc = {
                        "hospital": hospital,
                        "patient": patient,
                        "detected_at": datetime.utcnow().isoformat(),
                        "triggered": triggered
                    }
                    db.collection("alerts").add(alert_doc)
                    alert_producer.send(KAFKA_ALERTS, alert_doc)
                    logging.info(f"[ALERT] Triggered for {patient}: {triggered}")

            except Exception as e:
                logging.error(f"Error processing Kafka message: {e}")

    except Exception as e:
        logging.error(f"Alarm evaluator crashed: {e}")
        raise
