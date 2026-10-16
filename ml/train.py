"""Train Stock Risk model on SYNTHETIC data. Metrics are NOT real-world performance."""
import json, os
import joblib, numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from ml.features import FEATURES, rule_risk

MODEL_PATH = os.getenv("MODEL_PATH", "/tmp/risk_model.joblib")

def train(n=5000, seed=0):
    rng = np.random.default_rng(seed)
    X = np.c_[rng.integers(0, 600, n), rng.integers(20, 250, n), rng.integers(1, 40, n), rng.integers(1, 15, n)]
    y = rule_risk(X[:, 0], X[:, 1], X[:, 2], X[:, 3]).astype(object)
    flip = rng.random(n) < 0.05  # label noise so the task isn't trivially deterministic
    y[flip] = rng.choice(["LOW", "MEDIUM", "HIGH"], flip.sum())
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=seed)
    model = RandomForestClassifier(n_estimators=60, max_depth=8, random_state=seed).fit(Xtr, ytr)
    acc = float(model.score(Xte, yte))
    joblib.dump(model, MODEL_PATH)
    json.dump({"synthetic_holdout_accuracy": acc, "note": "synthetic data only"}, open(MODEL_PATH + ".json", "w"))
    return model, acc

if __name__ == "__main__":
    _, a = train()
    print(f"Saved {MODEL_PATH}. Synthetic holdout accuracy={a:.3f} (NOT real-world performance)")
