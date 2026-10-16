import json, os, pika
from kafka.admin import KafkaAdminClient, NewTopic
BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
TOPIC, QUEUE = "inventory-events", "inventory-alerts"

def ensure_topic():
    a = KafkaAdminClient(bootstrap_servers=BROKER)
    try:
        if TOPIC not in a.list_topics(): a.create_topics([NewTopic(TOPIC, 1, 1)])
    finally:
        a.close()

def redpanda_ok():
    try:
        a = KafkaAdminClient(bootstrap_servers=BROKER, request_timeout_ms=3000); t = TOPIC in a.list_topics(); a.close(); return t
    except Exception:
        return False

def _chan():
    c = pika.BlockingConnection(pika.URLParameters(RABBIT_URL)); ch = c.channel()
    ch.queue_declare(QUEUE, durable=True); return c, ch

def publish_alert(payload):
    c, ch = _chan()
    ch.basic_publish("", QUEUE, json.dumps(payload, default=str), pika.BasicProperties(delivery_mode=2)); c.close()

def queue_depth():
    try:
        c, ch = _chan(); n = ch.queue_declare(QUEUE, durable=True).method.message_count; c.close(); return n
    except Exception:
        return None
