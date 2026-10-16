import numpy as np
FEATURES = ["quantity_on_hand", "reorder_point", "observed_demand", "lead_time_days"]

def rule_risk(q, r, d, l):
    """Business rule used for ETL risk classification AND to label synthetic ML data."""
    q, r, d, l = map(np.asarray, (q, r, d, l))
    cover = q / np.clip(d, 1, None)
    return np.select([(q <= 0.5 * r) | (cover < l), q <= r], ["HIGH", "MEDIUM"], "LOW")
