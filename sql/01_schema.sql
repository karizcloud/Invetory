CREATE TABLE IF NOT EXISTS raw_inventory(
 id SERIAL PRIMARY KEY, run_id TEXT, batch_id TEXT, product_id TEXT, product_name TEXT, category TEXT,
 quantity_on_hand INT, reorder_point INT, observed_demand INT, lead_time_days INT, event_ts TIMESTAMP,
 loaded_at TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS processed_inventory(
 id SERIAL PRIMARY KEY, run_id TEXT, batch_id TEXT, product_id TEXT, product_name TEXT, category TEXT,
 quantity_on_hand INT, reorder_point INT, observed_demand INT, lead_time_days INT, event_ts TIMESTAMP,
 days_of_cover NUMERIC, stock_gap INT, suggested_reorder_qty INT, risk_rule TEXT, risk_ml TEXT,
 processed_at TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS rejected_records(
 id SERIAL PRIMARY KEY, source TEXT, run_id TEXT, batch_id TEXT, raw_data TEXT, reason TEXT, ts TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS batch_runs(
 run_id TEXT PRIMARY KEY, batch_size INT, status TEXT, extracted INT DEFAULT 0, valid INT DEFAULT 0,
 rejected INT DEFAULT 0, transformed INT DEFAULT 0, loaded INT DEFAULT 0, error TEXT,
 started_at TIMESTAMP DEFAULT now(), finished_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS stream_events(
 event_id TEXT PRIMARY KEY, product_id TEXT, product_name TEXT, category TEXT, quantity_on_hand INT,
 reorder_point INT, observed_demand INT, lead_time_days INT, event_ts TIMESTAMP,
 days_of_cover NUMERIC, stock_gap INT, suggested_reorder_qty INT, risk_rule TEXT, risk_ml TEXT,
 processed_at TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS alerts(
 id SERIAL PRIMARY KEY, event_id TEXT, product_id TEXT, risk TEXT, message TEXT, processed_at TIMESTAMP DEFAULT now());
CREATE TABLE IF NOT EXISTS pipeline_logs(
 id SERIAL PRIMARY KEY, ts TIMESTAMP DEFAULT now(), stage TEXT, component TEXT, operation TEXT,
 status TEXT, records INT DEFAULT 0, batch_id TEXT, message TEXT);
