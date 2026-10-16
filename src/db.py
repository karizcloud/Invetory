import os
import pandas as pd, psycopg2
from psycopg2.extras import execute_values

def conn(): return psycopg2.connect(os.getenv("DATABASE_URL", "postgresql://inventory:inventory@postgres:5432/inventory"))

def execute(sql, params=None):
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount

def query(sql, params=None):
    c = conn()
    try:
        with c.cursor() as cur:
            cur.execute(sql, params)
            return pd.DataFrame(cur.fetchall(), columns=[x[0] for x in cur.description])
    finally:
        c.close()

def insert_df(table, df):
    if df.empty: return 0
    cols = list(df.columns)
    rows = df.astype(object).where(df.notna(), None).values.tolist()
    with conn() as c, c.cursor() as cur:
        execute_values(cur, f"INSERT INTO {table} ({','.join(cols)}) VALUES %s", rows)
    return len(rows)

def log(stage, component, operation, status, records=0, message="", batch_id=None):
    try:
        execute("INSERT INTO pipeline_logs(stage,component,operation,status,records,batch_id,message) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                (stage, component, operation, status, records, batch_id, message))
    except Exception as e:
        print("log failed", e)
