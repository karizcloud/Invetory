import json, os
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np, pandas as pd
from src import db, metrics
from src.transform import validate_df, transform_df

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CSV = DATA_DIR / "inventory_sample.csv"
RAW_COLS = ["product_id","product_name","category","quantity_on_hand","reorder_point","observed_demand","lead_time_days","event_ts"]
PROC_COLS = RAW_COLS + ["days_of_cover","stock_gap","suggested_reorder_qty","risk_rule","risk_ml"]

def generate_csv(path=CSV, n=1000, seed=42, force=False):
    """Writes ~1000 SIMULATED records, ~3% intentionally corrupted to exercise validation."""
    path = Path(path)
    if path.exists() and not force: return path, False
    rng = np.random.default_rng(seed); path.parent.mkdir(parents=True, exist_ok=True)
    cats = ["Electronics", "Grocery", "Apparel", "Hardware", "Pharmacy"]; now = datetime.now()
    df = pd.DataFrame({
        "product_id": [f"P{i % 300:04d}" for i in range(n)],
        "product_name": [f"{cats[i % 5]} item {i % 300}" for i in range(n)],
        "category": [cats[i % 5] for i in range(n)],
        "quantity_on_hand": rng.integers(0, 500, n), "reorder_point": rng.integers(20, 200, n),
        "observed_demand": rng.integers(1, 30, n), "lead_time_days": rng.integers(1, 14, n),
        "timestamp": [(now - timedelta(minutes=int(m))).isoformat(timespec="seconds") for m in rng.integers(0, 10080, n)]})
    df = df.astype(object)
    for k, i in enumerate(rng.choice(n, n // 30, replace=False)):
        if k % 3 == 0: df.loc[i, "quantity_on_hand"] = -5
        elif k % 3 == 1: df.loc[i, "product_name"] = ""
        else: df.loc[i, "observed_demand"] = "N/A"
    df.to_csv(path, index=False)
    return path, True

def _dir(run_id):
    d = DATA_DIR / "staging" / run_id; d.mkdir(parents=True, exist_ok=True); return d

def _upd(run_id, **kw):
    sets = ",".join(f"{k}=%s" for k in kw)
    db.execute(f"UPDATE batch_runs SET {sets} WHERE run_id=%s", (*kw.values(), run_id))

def _bid(run_id, f): return f"{run_id}-B{f.stem.split('_')[1]}"

def t_generate():
    p, made = generate_csv()
    n = len(pd.read_csv(p))
    db.log("generation", "airflow", "generate_csv" if made else "locate_csv", "success", n, str(p))
    return n

def t_extract(run_id, batch_size):
    d = _dir(run_id)
    db.execute("INSERT INTO batch_runs(run_id,batch_size,status) VALUES(%s,%s,'running') ON CONFLICT (run_id) DO UPDATE SET batch_size=EXCLUDED.batch_size, status='running'", (run_id, batch_size))
    total = 0
    for i, chunk in enumerate(pd.read_csv(CSV, chunksize=batch_size, dtype=str), 1):
        chunk.to_csv(d / f"raw_{i:03d}.csv", index=False); total += len(chunk)
        db.log("ingestion", "airflow", "extract_batch", "success", len(chunk), batch_id=f"{run_id}-B{i:03d}")
    _upd(run_id, extracted=total); return total

def t_validate(run_id):
    d = _dir(run_id); ok_n = rej_n = 0
    for f in sorted(d.glob("raw_*.csv")):
        ok, rej = validate_df(pd.read_csv(f, dtype=str)); bid = _bid(run_id, f)
        ok.to_csv(d / f.name.replace("raw_", "valid_"), index=False)
        if len(rej):
            db.insert_df("rejected_records", pd.DataFrame({
                "source": "batch", "run_id": run_id, "batch_id": bid, "reason": rej.reason.values,
                "raw_data": rej.drop(columns="reason").astype(str).apply(lambda r: json.dumps(r.to_dict()), axis=1).values}))
        ok_n += len(ok); rej_n += len(rej)
        db.log("ingestion", "airflow", "validate_batch", "success", len(ok), f"rejected={len(rej)}", bid)
    _upd(run_id, valid=ok_n, rejected=rej_n); return ok_n, rej_n

def t_store_raw(run_id):
    d = _dir(run_id); n = 0
    for f in sorted(d.glob("valid_*.csv")):
        df = pd.read_csv(f).rename(columns={"timestamp": "event_ts"})[RAW_COLS]
        df["event_ts"] = pd.to_datetime(df.event_ts); df.insert(0, "batch_id", _bid(run_id, f)); df.insert(0, "run_id", run_id)
        n += db.insert_df("raw_inventory", df); db.log("storage", "airflow", "store_raw", "success", len(df), batch_id=_bid(run_id, f))
    return n

def t_transform(run_id):
    d = _dir(run_id); n = 0
    for f in sorted(d.glob("valid_*.csv")):
        out = transform_df(pd.read_csv(f)); out.to_csv(d / f.name.replace("valid_", "proc_"), index=False); n += len(out)
        db.log("transformation", "airflow", "transform_batch", "success", len(out), "risk+cover+reorder metrics", _bid(run_id, f))
    _upd(run_id, transformed=n); return n

def t_load(run_id):
    d = _dir(run_id); n = 0
    for f in sorted(d.glob("proc_*.csv")):
        df = pd.read_csv(f)[PROC_COLS]; df["event_ts"] = pd.to_datetime(df.event_ts)
        df.insert(0, "batch_id", _bid(run_id, f)); df.insert(0, "run_id", run_id)
        n += db.insert_df("processed_inventory", df); db.log("storage", "airflow", "load_processed", "success", len(df), batch_id=_bid(run_id, f))
    _upd(run_id, loaded=n); return n

def t_update_metrics(run_id):
    r = db.query("SELECT * FROM batch_runs WHERE run_id=%s", (run_id,)).iloc[0]
    metrics.incr("records_ingested", int(r.extracted)); metrics.incr("records_transformed", int(r.transformed))
    metrics.incr("records_loaded", int(r.loaded)); metrics.incr("rejected_records", int(r.rejected))
    _upd(run_id, status="success", finished_at=datetime.now())
    db.log("serving", "airflow", "update_metrics", "success", int(r.loaded), "redis metrics updated", run_id)

def mark_failed(run_id, err):
    try:
        _upd(run_id, status="failed", error=str(err)[:500], finished_at=datetime.now())
        db.log("ingestion", "airflow", "task_failed", "error", 0, str(err)[:500], run_id)
    except Exception: pass
