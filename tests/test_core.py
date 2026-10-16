import pandas as pd
from src.transform import validate_df, transform_df
from ml.features import rule_risk

def _row(**o):
    r = dict(product_id="p1", product_name=" widget ", category="tools", quantity_on_hand=5, reorder_point=50, observed_demand=10, lead_time_days=5, timestamp="2025-01-01T10:00:00")
    r.update(o); return r

def test_validation_rejects_bad():
    df = pd.DataFrame([_row(), _row(quantity_on_hand=-1), _row(product_name=""), _row(observed_demand="N/A"), _row(timestamp="zzz")])
    ok, rej = validate_df(df)
    assert len(ok) == 1 and len(rej) == 4

def test_transform_metrics_and_risk():
    ok, _ = validate_df(pd.DataFrame([_row(), _row(quantity_on_hand=400)]))
    out = transform_df(ok)
    assert out.product_name.iloc[0] == "Widget" and out.days_of_cover.iloc[0] == 0.5
    assert list(out.risk_rule) == ["HIGH", "LOW"]

def test_ml_predicts_valid_labels():
    ok, _ = validate_df(pd.DataFrame([_row()]))
    assert transform_df(ok).risk_ml.iloc[0] in {"LOW", "MEDIUM", "HIGH"}

def test_rule_risk():
    assert rule_risk([100], [50], [1], [2])[0] == "LOW"
