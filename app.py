import os, requests, pandas as pd, streamlit as st
from streamlit_autorefresh import st_autorefresh
from src import db, metrics, bus

st.set_page_config("InventoryFlow", layout="wide")
AF = os.getenv("AIRFLOW_URL", "http://airflow:8080"); AUTH = (os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PASSWORD", "admin"))
DAG = "inventory_batch_etl"

def q(sql, p=None):
    try: return db.query(sql, p)
    except Exception as e: st.error(f"DB error: {e}"); return pd.DataFrame()
def M():
    try: return metrics.all_metrics()
    except Exception as e: st.error(f"Redis error: {e}"); return {}
def af(method, path, **kw):
    try: return requests.request(method, f"{AF}/api/v1{path}", auth=AUTH, timeout=5, **kw)
    except Exception: return None

page = st.sidebar.radio("InventoryFlow", ["Pipeline Overview", "Batch Processing", "Stream Processing", "Storage & Transformation", "Data Serving & ML", "Pipeline Logs"])
st.sidebar.markdown("[Airflow UI](http://localhost:8081) · [Redpanda Console](http://localhost:8080) · [RabbitMQ UI](http://localhost:15672)")
st.title("InventoryFlow: Intelligent Inventory Data Pipeline")
m = M()

if page == "Pipeline Overview":
    st.caption("All values come from PostgreSQL / Redis. Streaming events are SIMULATED data.")
    st.graphviz_chart('digraph{rankdir=LR;node[shape=box,style=rounded];CSV->Airflow->PG[label=""];Gen[label="Event Generator (simulated)"];Gen->Redpanda->Consumer->PG[label="PostgreSQL"];Consumer->Redis;Consumer->RabbitMQ->AlertWorker->PG;Airflow->Redis;PG->Streamlit;Redis->Streamlit}')
    s = q("SELECT stage, max(ts) last_ts, sum(records) recs, (array_agg(status ORDER BY ts DESC))[1] status FROM pipeline_logs GROUP BY stage")
    s = s.set_index("stage") if len(s) else None
    cols = st.columns(5)
    for c, (name, k) in zip(cols, [("Data Generation","generation"),("Data Ingestion","ingestion"),("Data Storage","storage"),("Data Transformation","transformation"),("Data Serving","serving")]):
        with c.container(border=True):
            st.subheader(name)
            if s is not None and k in s.index:
                r = s.loc[k]; st.write(f"**Status:** {r.status}"); st.write(f"**Records:** {int(r.recs)}"); st.caption(f"Last: {r.last_ts:%Y-%m-%d %H:%M:%S}")
            else: st.write("No activity yet")
    st.subheader("Live metrics (Redis)")
    labels = ["records_ingested","records_transformed","records_loaded","stream_events_processed","rejected_records","high_risk_alerts","stream_status"]
    for c, k in zip(st.columns(len(labels)), labels): c.metric(k.replace("_", " "), m.get(k, 0))
    st.subheader("Service connectivity")
    c1, c2, c3 = st.columns(3)
    try: metrics.r().ping(); c1.success("Redis: connected")
    except Exception: c1.error("Redis: down")
    c2.success("Redpanda: topic inventory-events OK") if bus.redpanda_ok() else c2.error("Redpanda: unreachable / topic missing")
    d = bus.queue_depth(); c3.success(f"RabbitMQ: connected ({d} pending)") if d is not None else c3.error("RabbitMQ: down")

elif page == "Batch Processing":
    size = st.number_input("Batch size", 50, 1000, 200, 50)
    if st.button("Run Batch ETL (trigger Airflow DAG)"):
        r = af("POST", f"/dags/{DAG}/dagRuns", json={"conf": {"batch_size": int(size)}})
        st.success("Triggered") if r is not None and r.ok else st.error(f"Airflow API failed ({getattr(r,'status_code','no connection')}). Trigger manually in the Airflow UI: http://localhost:8081")
    r = af("GET", f"/dags/{DAG}/dagRuns?order_by=-execution_date&limit=1")
    if r is not None and r.ok and r.json()["dag_runs"]:
        run = r.json()["dag_runs"][0]; st.write(f"Latest DAG run **{run['dag_run_id']}** — state: **{run['state']}**")
        t = af("GET", f"/dags/{DAG}/dagRuns/{requests.utils.quote(run['dag_run_id'], safe='')}/taskInstances")
        if t is not None and t.ok:
            ti = pd.DataFrame(t.json()["task_instances"])[["task_id", "state", "start_date", "end_date"]]
            st.progress(float((ti.state == "success").mean())); st.dataframe(ti, use_container_width=True)
    else: st.info("Airflow not reachable or no runs yet.")
    st.subheader("Batch runs (PostgreSQL)"); st.dataframe(q("SELECT * FROM batch_runs ORDER BY started_at DESC LIMIT 10"), use_container_width=True)
    a, b = st.columns(2)
    a.subheader("Raw records"); a.dataframe(q("SELECT product_id,product_name,category,quantity_on_hand,reorder_point,observed_demand,lead_time_days,batch_id FROM raw_inventory ORDER BY id DESC LIMIT 15"))
    b.subheader("Processed records"); b.dataframe(q("SELECT product_id,product_name,category,quantity_on_hand,days_of_cover,stock_gap,suggested_reorder_qty,risk_rule,risk_ml FROM processed_inventory ORDER BY id DESC LIMIT 15"))
    st.subheader("Transformation logs"); st.dataframe(q("SELECT ts,operation,status,records,batch_id,message FROM pipeline_logs WHERE component='airflow' ORDER BY id DESC LIMIT 30"), use_container_width=True)

elif page == "Stream Processing":
    on = metrics.stream_enabled(); a, b, c = st.columns(3)
    if a.button("Start Stream"): metrics.set_stream(True); st.rerun()
    if b.button("Stop Stream"): metrics.set_stream(False); st.rerun()
    c.metric("Stream", "RUNNING" if on else "STOPPED")
    if on: st_autorefresh(interval=3000, key="stream")
    st.warning("Events below are SIMULATED by the producer worker.")
    rate = q("SELECT count(*) n FROM stream_events WHERE processed_at > now() - interval '60 seconds'")
    cols = st.columns(6)
    for col, (l, v) in zip(cols, [("Produced", m.get("events_produced", 0)), ("Processed", m.get("stream_events_processed", 0)), ("Rejected", m.get("rejected_records", 0)),
                                   ("Duplicates skipped", m.get("duplicates_skipped", 0)), ("Consumer errors", m.get("consumer_errors", 0)), ("Events/min", int(rate.n[0]) if len(rate) else 0)]): col.metric(l, v)
    st.write(f"Redpanda: {'connected' if bus.redpanda_ok() else 'unreachable'} · Last event: {m.get('last_event_ts', '-')}")
    st.dataframe(q("SELECT processed_at,event_id,product_id,quantity_on_hand,days_of_cover,risk_rule,risk_ml FROM stream_events ORDER BY processed_at DESC LIMIT 20"), use_container_width=True)
    st.subheader("Recent stream rejections"); st.dataframe(q("SELECT ts,reason,raw_data FROM rejected_records WHERE source='stream' ORDER BY id DESC LIMIT 10"), use_container_width=True)
    st.subheader("RabbitMQ alerts"); d = bus.queue_depth()
    x, y = st.columns(2); x.metric("Pending (queue)", "unavailable" if d is None else d); y.metric("Processed", m.get("alerts_processed", 0))
    st.dataframe(q("SELECT processed_at,product_id,risk,message FROM alerts ORDER BY id DESC LIMIT 10"), use_container_width=True)

elif page == "Storage & Transformation":
    cnt = q("SELECT (SELECT count(*) FROM raw_inventory) raw,(SELECT count(*) FROM processed_inventory) processed,(SELECT count(*) FROM stream_events) stream,(SELECT count(*) FROM rejected_records) rejected")
    for c, k in zip(st.columns(4), cnt.columns): c.metric(f"{k} rows", int(cnt[k][0]) if len(cnt) else 0)
    st.subheader("Rejected records & validation errors"); st.dataframe(q("SELECT ts,source,batch_id,reason,raw_data FROM rejected_records ORDER BY id DESC LIMIT 20"), use_container_width=True)
    st.dataframe(q("SELECT reason, count(*) FROM rejected_records GROUP BY reason"))
    st.subheader("Before → after (same batch rows)")
    st.dataframe(q("""SELECT r.product_id, r.product_name raw_name, p.product_name clean_name, r.category raw_cat, p.category clean_cat,
        r.quantity_on_hand, p.days_of_cover, p.stock_gap, p.suggested_reorder_qty, p.risk_rule FROM raw_inventory r
        JOIN processed_inventory p ON p.run_id=r.run_id AND p.batch_id=r.batch_id AND p.product_id=r.product_id AND p.event_ts=r.event_ts LIMIT 15"""), use_container_width=True)
    st.markdown("**Transformations applied:** trim/Title-case names & categories, upper-case IDs, `days_of_cover = qty / max(demand,1)`, `stock_gap = reorder_point − qty`, `suggested_reorder_qty = max(0, demand×lead + reorder_point − qty)`, rule risk (HIGH: qty ≤ 50% reorder or cover < lead time; MEDIUM: qty ≤ reorder; else LOW), plus ML risk.")

elif page == "Data Serving & ML":
    st.caption("ML model is trained on SYNTHETIC data; its accuracy does not reflect real-world performance.")
    s = q("SELECT count(*) total, count(*) FILTER (WHERE quantity_on_hand<=reorder_point) low_stock, count(*) FILTER (WHERE risk_rule='HIGH') high_risk, count(*) FILTER (WHERE risk_ml='HIGH') ml_high FROM processed_inventory")
    for c, k in zip(st.columns(4), s.columns): c.metric(k.replace("_", " "), int(s[k][0]) if len(s) else 0)
    cat = q("SELECT category, count(*) records, round(avg(days_of_cover),2) avg_cover, count(*) FILTER (WHERE risk_rule='HIGH') high_risk FROM processed_inventory GROUP BY category")
    if len(cat): st.bar_chart(cat.set_index("category")[["records", "high_risk"]]); st.dataframe(cat)
    st.subheader("Processed inventory with rule vs ML risk"); st.dataframe(q("SELECT product_id,product_name,category,quantity_on_hand,reorder_point,observed_demand,lead_time_days,days_of_cover,risk_rule,risk_ml FROM processed_inventory ORDER BY id DESC LIMIT 200"), use_container_width=True)

else:
    st.dataframe(q("SELECT ts,stage,component,operation,status,records,batch_id,message FROM (SELECT * FROM pipeline_logs ORDER BY id DESC LIMIT 200) t ORDER BY id"), use_container_width=True)
