import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
from io import BytesIO
from datetime import datetime
import os

# 🔐 PostgreSQL connection config
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_DB = "airflow"
TABLE_NAME = "fact_credit_growth"

# 🔐 MinIO config
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "kredit-data"

# Lokasi file .sql
SQL_PATH = "/opt/airflow/dags/sql/create_fact_credit_growth.sql"

def read_parquet_from_minio(date_str):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    parquet_path = f"processed/{datetime.today().strftime('%Y-%m-%d')}/kredit_mom_flagged.parquet"
    print(f"⬇️ Downloading parquet from MinIO: {parquet_path}")
    response = client.get_object(MINIO_BUCKET, parquet_path)
    return pd.read_parquet(BytesIO(response.read()))

def run_sql_script(filepath):
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    with engine.connect() as conn:
        with open(filepath, "r") as f:
            query = f.read()
        conn.execute(text(query))
        print(f"✅ Executed SQL script: {filepath}")

def load_to_postgres(df):
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print(f"✅ Loaded data into PostgreSQL table: {TABLE_NAME}")

if __name__ == "__main__":
    try:
        today_str = datetime.today().strftime("%Y%m%d")

        # STEP 1: Buat table (jika belum ada)
        run_sql_script(SQL_PATH)

        # STEP 2: Ambil data dari MinIO
        df = read_parquet_from_minio(today_str)

        # STEP 3: Masukkan ke PostgreSQL
        load_to_postgres(df)

    except Exception as e:
        print(f"🚨 Error during PostgreSQL load: {e}")
