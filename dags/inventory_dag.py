import re
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src import etl

def rid(c): return re.sub(r"\W", "_", c["run_id"])
def bs(c): return int((c["dag_run"].conf or {}).get("batch_size", 200))
def fail(c): etl.mark_failed(rid(c), c.get("exception"))

with DAG("inventory_batch_etl", start_date=datetime(2024, 1, 1), schedule=None, catchup=False,
         default_args={"on_failure_callback": fail}, tags=["snde"]) as dag:
    gen = PythonOperator(task_id="generate_or_locate_csv", python_callable=lambda **c: etl.t_generate())
    ext = PythonOperator(task_id="extract_batches", python_callable=lambda **c: etl.t_extract(rid(c), bs(c)))
    val = PythonOperator(task_id="validate_records", python_callable=lambda **c: etl.t_validate(rid(c)))
    raw = PythonOperator(task_id="store_raw", python_callable=lambda **c: etl.t_store_raw(rid(c)))
    trf = PythonOperator(task_id="transform_records", python_callable=lambda **c: etl.t_transform(rid(c)))
    lod = PythonOperator(task_id="load_processed", python_callable=lambda **c: etl.t_load(rid(c)))
    met = PythonOperator(task_id="update_metrics", python_callable=lambda **c: etl.t_update_metrics(rid(c)))
    gen >> ext >> val >> raw >> trf >> lod >> met
