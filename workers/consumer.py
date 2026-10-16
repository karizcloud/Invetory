import json, time
import pandas as pd
from kafka import KafkaConsumer
from src import bus, db, metrics
from src.transform import validate_df, transform_df
COLS = ["event_id","product_id","product_name","category","quantity_on_hand","reorder_point","observed_demand","lead_time_days","event_ts","days_of_cover","stock_gap","suggested_reorder_qty","risk_rule","risk_ml"]

def handle(raw):
    df = pd.DataFrame([raw]); good, bad = validate_df(df)
    if not raw.get("event_id"): bad = df.assign(reason="missing event_id"); good = good.iloc[0:0]
    if len(bad):
        db.insert_df("rejected_records", pd.DataFrame({"source": ["stream"], "raw_data": [json.dumps(raw)], "reason": [bad.reason.iloc[0]]}))
        metrics.incr("rejected_records"); return "rejected"
    row = transform_df(good).iloc[0][COLS]
    vals = [None if pd.isna(v) else (v.item() if hasattr(v, "item") else v) for v in row]
    n = db.execute(f"INSERT INTO stream_events({','.join(COLS)}) VALUES({','.join(['%s']*len(COLS))}) ON CONFLICT (event_id) DO NOTHING", vals)
    if n == 0: metrics.incr("duplicates_skipped"); return "duplicate"
    metrics.incr("stream_events_processed"); metrics.incr("records_transformed"); metrics.incr("records_loaded")
    if row.risk_rule == "HIGH":
        bus.publish_alert({"event_id": row.event_id, "product_id": row.product_id, "risk": "HIGH",
                           "message": f"{row.product_name}: qty {row.quantity_on_hand}, cover {row.days_of_cover}d < lead {row.lead_time_days}d or below 50% reorder"})
        metrics.incr("high_risk_alerts")
    return "ok"

def main():
    while True:
        try: bus.ensure_topic(); c = KafkaConsumer(bus.TOPIC, bootstrap_servers=bus.BROKER, group_id="inventory-consumer", auto_offset_reset="earliest", enable_auto_commit=False); break
        except Exception as ex: print("waiting for redpanda", ex); time.sleep(3)
    k = 0
    for msg in c:
        try:
            handle(json.loads(msg.value)); k += 1
            if k % 20 == 0: db.log("ingestion", "consumer", "consume_events", "success", 20, "stream")
        except Exception as ex:
            metrics.incr("consumer_errors"); db.log("ingestion", "consumer", "consume_error", "error", 0, str(ex)[:300])
        c.commit()

if __name__ == "__main__": main()
