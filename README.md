# InventoryFlow — SNDE demo

Stack: Streamlit · Airflow 2.9 · Redpanda · RabbitMQ · Redis · PostgreSQL · Pandas · scikit-learn (Docker Compose).

## Run
```
cp .env.example .env
chmod -R 777 data            # Airflow container writes staging files here
docker compose up -d --build # first start takes a few minutes (Airflow pip installs)
docker compose ps            # wait until airflow is healthy
```
Stop: `docker compose down` (add `-v` to wipe PostgreSQL data).

## URLs
Dashboard http://localhost:8501 · Airflow http://localhost:8081 (admin/admin) · Redpanda Console http://localhost:8080 · RabbitMQ http://localhost:15672 (guest/guest)

## Demo script
1. **Overview**: five stage cards + connectivity (empty until something runs).
2. **Batch ETL**: *Batch Processing* → Run Batch ETL (batch size 200). Watch task states, `batch_runs` counts (extracted 1000, ~33 rejected), raw vs processed tables. Fallback: trigger `inventory_batch_etl` in the Airflow UI.
3. **Streaming**: *Stream Processing* → Start Stream. Simulated events appear every 1–2 s; processed/rejected/duplicate counters rise. Stop Stream pauses the producer via Redis flag `stream:enabled`.
4. **RabbitMQ alerts**: HIGH-risk stream events are published to `inventory-alerts`; alert-worker acks them; see pending/processed and details on the same page.
5. **ML**: *Data Serving & ML* shows rule risk vs ML risk (model trained on synthetic data, auto-trained on first use; `python -m ml.train` to retrain).
6. **Logs**: *Pipeline Logs*.

## Tests
`pip install -r requirements.txt && PYTHONPATH=. pytest -q` (unit tests need no services).

## Known limits
- Alerts are published from the stream path only, not from batch loads.
- Pipeline logs for the stream are summarized every 20 events, not per event.
