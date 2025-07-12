
import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio
from io import BytesIO
from datetime import datetime

# 🔐 PostgreSQL connection config
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_DB = "airflow"
TABLE_NAME = "fact_credit_growth_clean"

# 🔐 MinIO config
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "kredit-data"

# SQL file paths
SQL_DIM_SEKTOR_PATH = "/opt/airflow/dags/sql/create_table_dim_sektor.sql"
SQL_DIM_BANK_PATH = "/opt/airflow/dags/sql/create_table_dim_bank_type.sql"
SQL_FACT_CLEAN_PATH = "/opt/airflow/dags/sql/create_fact_credit_growth_clean.sql"
SQL_FACT_RAW_PATH = "/opt/airflow/dags/sql/create_fact_credit_growth.sql"

def read_parquet_from_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    date_path = datetime.today().strftime('%Y-%m-%d')
    parquet_path = f"processed/{date_path}/kredit_mom_flagged.parquet"
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
    
    # Tambahkan kolom tanggal dengan nilai hari ini
    today = datetime.today().date()
    df["date"] = today

    # Pastikan kolom is_abnormal string
    df["is_abnormal"] = df["is_abnormal"].apply(lambda x: "TRUE" if x else "FALSE")

    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print(f"✅ Loaded data into PostgreSQL table: {TABLE_NAME}")

if __name__ == "__main__":
    try:
        # Step 1: Create tables if not exist
        run_sql_script(SQL_DIM_SEKTOR_PATH)
        run_sql_script(SQL_DIM_BANK_PATH)
        run_sql_script(SQL_FACT_CLEAN_PATH)

        # Step 2: Read from MinIO
        df = read_parquet_from_minio()

        # Step 3: Load to PostgreSQL
        load_to_postgres(df)

    except Exception as e:
        print(f"🚨 Error during PostgreSQL load: {e}")

        