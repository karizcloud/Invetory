"""Simulated inventory event generator. Runs only while Redis flag stream:enabled == 1."""
import json, random, time, uuid
from datetime import datetime
from kafka import KafkaProducer
from src import bus, db, metrics

def event():
    pid = random.randint(1, 50)
    e = {"event_id": str(uuid.uuid4()), "product_id": f"P{pid:04d}", "product_name": f"simulated item {pid}",
         "category": random.choice(["electronics", "grocery", "apparel", "hardware", "pharmacy"]),
         "quantity_on_hand": random.randint(0, 400), "reorder_point": random.randint(20, 200),
         "observed_demand": random.randint(1, 30), "lead_time_days": random.randint(1, 14),
         "timestamp": datetime.now().isoformat(timespec="seconds"), "source": "SIMULATED"}
    if random.random() < 0.08: e["quantity_on_hand"] = -random.randint(1, 20)  # invalid on purpose
    return e

def main():
    while True:
        try: bus.ensure_topic(); p = KafkaProducer(bootstrap_servers=bus.BROKER, value_serializer=lambda v: json.dumps(v).encode()); break
        except Exception as ex: print("waiting for redpanda", ex); time.sleep(3)
    last, n, was_on = None, 0, False
    while True:
        on = metrics.stream_enabled()
        if on != was_on:
            db.log("generation", "producer", "stream_start" if on else "stream_stop", "success"); was_on = on
        if not on: time.sleep(1); continue
        e = last if (last and random.random() < 0.05) else event()  # 5% duplicate redelivery
        p.send(bus.TOPIC, e); p.flush(); last = e; n += 1
        metrics.incr("events_produced"); metrics.setv("last_event_ts", datetime.now().isoformat(timespec="seconds"))
        if n % 20 == 0: db.log("generation", "producer", "produce_events", "success", 20, "SIMULATED events")
        time.sleep(random.uniform(1, 2))

if __name__ == "__main__": main()
