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

def transform_mom_and_flag():
    try:
        # === Step 1: Download parquet dari MinIO ===
        date_folder = datetime.today().strftime('%Y-%m-%d')
        parquet_path = f"processed/{date_folder}/combined_kredit_sektoral.parquet"

        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        response = client.get_object(MINIO_BUCKET, parquet_path)
        df = pd.read_parquet(BytesIO(response.read()))
        print("📥 File berhasil diambil dari MinIO")

        # === Step 2: Pastikan kolom numerik ===
        for col in ['jan', 'feb', 'mar', 'apr', 'may']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # === Step 3: Hitung MoM Growth ===
        df["mom_feb"] = ((df["feb"] - df["jan"]) / df["jan"]) * 100
        df["mom_mar"] = ((df["mar"] - df["feb"]) / df["feb"]) * 100
        df["mom_apr"] = ((df["apr"] - df["mar"]) / df["mar"]) * 100
        df["mom_may"] = ((df["may"] - df["apr"]) / df["apr"]) * 100

        df[["mom_feb", "mom_mar", "mom_apr", "mom_may"]] = df[["mom_feb", "mom_mar", "mom_apr", "mom_may"]].round(2)

        # === Step 4: Flag abnormal jika >15% ===
        df["is_abnormal"] = df[["mom_feb", "mom_mar", "mom_apr", "mom_may"]].gt(15).any(axis=1)

        # === Step 5: Upload kembali ke MinIO ===
        out_path = f"processed/{date_folder}/kredit_mom_flagged.parquet"
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(MINIO_BUCKET, out_path, buffer, length=-1, part_size=10*1024*1024)
        print(f"✅ MoM data uploaded to MinIO: {out_path}")

    except Exception as e:
        print(f"🚨 Error in transform_mom_and_flag: {e}")
