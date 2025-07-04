import os
import requests
from datetime import datetime
from minio import Minio
from io import BytesIO

# Konfigurasi URL dan MinIO
EXCEL_URL = "https://www.bi.go.id/SEKI/tabel/TABEL1_5.xls"
BUCKET_NAME = "kredit-data"
FOLDER_NAME = "raw"
DATE_STR = datetime.today().strftime('%Y-%m-%d')
OBJECT_NAME = f"{FOLDER_NAME}/{DATE_STR}/TABEL1_5_{DATE_STR.replace('-', '')}.xls"

# Konfigurasi MinIO client
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def download_excel_to_minio(url):
    print(f"⬇️  Mengunduh file Excel dari {url}...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        file_bytes = BytesIO(response.content)

        # Pastikan bucket ada
        found = client.bucket_exists(BUCKET_NAME)
        if not found:
            client.make_bucket(BUCKET_NAME)

        # Upload ke MinIO
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            data=file_bytes,
            length=len(response.content),
            content_type="application/vnd.ms-excel"
        )
        print(f"✅ File berhasil diunggah ke MinIO: {BUCKET_NAME}/{OBJECT_NAME}")
        return OBJECT_NAME
    else:
        raise Exception(f"❌ Gagal mengunduh file. Status code: {response.status_code}")

if __name__ == "__main__":
    try:
        object_path = download_excel_to_minio(EXCEL_URL)
    except Exception as e:
        print(f"🚨 Terjadi kesalahan: {e}")
