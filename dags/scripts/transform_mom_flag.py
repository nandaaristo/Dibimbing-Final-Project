import pandas as pd
import numpy as np
from datetime import datetime
from minio import Minio
from io import BytesIO

# MinIO Config
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "kredit-data"

def download_parquet_from_minio(date_str):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    parquet_path = f"processed/{datetime.today().strftime('%Y-%m-%d')}/combined_kredit_sektoral.parquet"
    response = client.get_object(MINIO_BUCKET, parquet_path)
    return pd.read_parquet(BytesIO(response.read()))

def calculate_mom(df):
    # Pastikan kolom numerik
    for col in ['jan', 'feb', 'mar', 'apr', 'may']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Hitung MoM
    df["mom_feb"] = ((df["feb"] - df["jan"]) / df["jan"]) * 100
    df["mom_mar"] = ((df["mar"] - df["feb"]) / df["feb"]) * 100
    df["mom_apr"] = ((df["apr"] - df["mar"]) / df["mar"]) * 100
    df["mom_may"] = ((df["may"] - df["apr"]) / df["apr"]) * 100

    # 🔁 Bulatkan ke 2 desimal
    df[["mom_feb", "mom_mar", "mom_apr", "mom_may"]] = df[["mom_feb", "mom_mar", "mom_apr", "mom_may"]].round(2)

    # Deteksi abnormal
    df["is_abnormal"] = df[["mom_feb", "mom_mar", "mom_apr", "mom_may"]].gt(15).any(axis=1)

    return df

def save_mom_to_minio(df, date_str):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    out_path = f"processed/{datetime.today().strftime('%Y-%m-%d')}/kredit_mom_flagged.parquet"
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(MINIO_BUCKET, out_path, buffer, length=-1, part_size=10*1024*1024)
    print(f"✅ MoM data uploaded to MinIO: {out_path}")

if __name__ == "__main__":
    try:
        today_str = datetime.today().strftime("%Y%m%d")
        df = download_parquet_from_minio(today_str)
        df = calculate_mom(df)
        save_mom_to_minio(df, today_str)
    except Exception as e:
        print(f"🚨 Error during MoM transformation: {e}")
