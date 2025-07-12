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
TABLE_NAME = "fact_credit_growth"

# 🔐 MinIO config
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "kredit-data"

# SQL file paths
SQL_DIM_SEKTOR_PATH = "/opt/airflow/dags/sql/create_table_dim_sektor.sql"
SQL_DIM_BANK_PATH = "/opt/airflow/dags/sql/create_table_dim_bank_type.sql"
SQL_FACT_CLEAN_PATH = "/opt/airflow/dags/sql/create_fact_credit_growth_clean.sql"
SQL_FACT_PATH = "/opt/airflow/dags/sql/create_fact_credit_growth.sql"

def load_fact_credit_growth_clean():
    try:
        # STEP 1: Create table jika belum ada
        print("📄 Running SQL create table scripts...")
        engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        with engine.connect() as conn:
            for sql_path in [SQL_DIM_SEKTOR_PATH, SQL_DIM_BANK_PATH, SQL_FACT_CLEAN_PATH]:
                with open(sql_path, "r") as f:
                    conn.execute(text(f.read()))
                print(f"✅ Executed SQL: {sql_path}")

        # STEP 2: Ambil Parquet dari MinIO
        date_path = datetime.today().strftime('%Y-%m-%d')
        parquet_path = f"processed/{date_path}/kredit_mom_flagged.parquet"
        print(f"⬇️ Downloading parquet from MinIO: {parquet_path}")
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        response = client.get_object(MINIO_BUCKET, parquet_path)
        df = pd.read_parquet(BytesIO(response.read()))

        # STEP 3: Tambah kolom tanggal dan konversi bool
        today = datetime.today().date()
        df["date"] = today
        df["is_abnormal"] = df["is_abnormal"].apply(lambda x: "TRUE" if x else "FALSE")

        # STEP 4: Load ke PostgreSQL
        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        print(f"✅ Loaded data into PostgreSQL table: {TABLE_NAME}")

    except Exception as e:
        print(f"🚨 Error in load_fact_credit_growth_clean: {e}")
