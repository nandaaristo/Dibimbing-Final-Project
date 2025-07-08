import os
import requests
from datetime import datetime
from minio import Minio
from minio.error import S3Error

# URL Excel BI
EXCEL_URL = "https://www.bi.go.id/SEKI/tabel/TABEL1_5.xls"

# Konfigurasi MinIO
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "kredit-data"

def download_excel() -> str:
    date_str = datetime.today().strftime('%Y%m%d')
    filename = f"TABEL1_5_{date_str}.xls"
    local_path = f"/tmp/{filename}"

    headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"⬇️  Downloading Excel from {EXCEL_URL}")
    r = requests.get(EXCEL_URL, headers=headers)
    r.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(r.content)
    print(f"✅ Saved to {local_path}")
    return local_path

def upload_to_minio(local_file_path: str):
    date_folder = datetime.today().strftime('%Y-%m-%d')
    filename = os.path.basename(local_file_path)
    object_path = f"raw/{date_folder}/{filename}"

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Create bucket if not exists
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    client.fput_object(
        MINIO_BUCKET, object_path, local_file_path
    )
    print(f"📤 Uploaded to MinIO: {object_path}")

if __name__ == "__main__":
    try:
        file_path = download_excel()
        upload_to_minio(file_path)
    except Exception as e:
        print(f"🚨 Error: {e}")
