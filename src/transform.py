import numpy as np, pandas as pd
from ml.features import rule_risk
REQ = ["product_id","product_name","category","quantity_on_hand","reorder_point","observed_demand","lead_time_days","timestamp"]
NUM = ["quantity_on_hand","reorder_point","observed_demand","lead_time_days"]
TXT = ["product_id","product_name","category"]

def validate_df(df):
    df = df.copy()
    for c in REQ:
        if c not in df: df[c] = None
    reason = pd.Series("", index=df.index)
    miss = df[REQ].isna().any(axis=1) | df[TXT].astype(str).apply(lambda s: s.str.strip() == "").any(axis=1)
    reason[miss] = "missing required field"
    for c in NUM: df[c] = pd.to_numeric(df[c], errors="coerce")
    reason[df[NUM].isna().any(axis=1) & (reason == "")] = "non-numeric value"
    reason[(df[NUM] < 0).any(axis=1) & (reason == "")] = "negative value"
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    reason[ts.isna() & (reason == "")] = "invalid timestamp"
    df["timestamp"] = ts
    ok = reason == ""
    good = df[ok].copy()
    good[NUM] = good[NUM].astype(int)
    return good, df[~ok].assign(reason=reason[~ok])

def transform_df(df):
    d = df.rename(columns={"timestamp": "event_ts"}).copy()
    d["product_id"] = d.product_id.astype(str).str.strip().str.upper()
    d["product_name"] = d.product_name.astype(str).str.strip().str.title()
    d["category"] = d.category.astype(str).str.strip().str.title()
    dem = d.observed_demand.clip(lower=1)
    d["days_of_cover"] = (d.quantity_on_hand / dem).round(2)
    d["stock_gap"] = d.reorder_point - d.quantity_on_hand
    d["suggested_reorder_qty"] = (dem * d.lead_time_days + d.reorder_point - d.quantity_on_hand).clip(lower=0).astype(int)
    d["risk_rule"] = rule_risk(d.quantity_on_hand, d.reorder_point, d.observed_demand, d.lead_time_days)
    try:
        from ml.predict import predict_risk
        d["risk_ml"] = predict_risk(d)
    except Exception:
        d["risk_ml"] = None  # ML failure never blocks ETL
    return d
