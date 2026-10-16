import os
import joblib
from ml.features import FEATURES
from ml.train import MODEL_PATH, train
_model = None

def _get():
    global _model
    if _model is None:
        try:
            _model = joblib.load(MODEL_PATH)
        except Exception:
            _model, _ = train()
    return _model

def predict_risk(df):
    """Returns list of LOW/MEDIUM/HIGH. Callers must wrap in try/except (ML must not break ETL)."""
    return list(_get().predict(df[FEATURES].astype(int).values))
