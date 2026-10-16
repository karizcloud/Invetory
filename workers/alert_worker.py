import json, time, pika
from src import bus, db, metrics

def main():
    while True:
        try: conn, ch = bus._chan(); break
        except Exception as ex: print("waiting for rabbitmq", ex); time.sleep(3)
    def cb(ch, method, props, body):
        try:
            a = json.loads(body)
            db.execute("INSERT INTO alerts(event_id,product_id,risk,message) VALUES(%s,%s,%s,%s)", (a["event_id"], a["product_id"], a["risk"], a["message"]))
            metrics.incr("alerts_processed"); ch.basic_ack(method.delivery_tag)
        except Exception as ex:
            db.log("serving", "alert-worker", "alert_error", "error", 0, str(ex)[:300]); ch.basic_nack(method.delivery_tag, requeue=False)
    ch.basic_qos(prefetch_count=10); ch.basic_consume(bus.QUEUE, cb); ch.start_consuming()

if __name__ == "__main__": main()
