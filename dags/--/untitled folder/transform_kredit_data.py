
import pandas as pd
import os
from datetime import datetime
from minio import Minio
from io import BytesIO

# ===== Konfigurasi MinIO =====
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "kredit-data"

# ===== Nama Sheet yang akan digabung =====
SHEET_LIST = ["I.5_1", "I.5_2", "I.5_3"]

def download_excel_from_minio(date_str):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    excel_filename = f"TABEL1_5_{date_str}.xls"
    object_path = f"raw/{datetime.today().strftime('%Y-%m-%d')}/{excel_filename}"

    print(f"⬇️ Downloading Excel from MinIO: {object_path}")
    response = client.get_object(MINIO_BUCKET, object_path)
    return BytesIO(response.read())

def transform_and_merge_sheets(excel_io):
    combined_df = pd.DataFrame()

    for sheet_name in SHEET_LIST:
        
        df_raw = pd.read_excel(excel_io, sheet_name=sheet_name, header=None, dtype=str)

        # Ambil baris ke-6 sampai ke-55
        df_clean = df_raw.iloc[5:55]

        # ✅ Potong hanya kolom yang diperlukan
        df_clean = df_clean.iloc[:, [2, 3, 4, 5, 6, 7, 8]]

        # ✅ Tambah ke combined_df
        combined_df = pd.concat([combined_df, df_clean], ignore_index=True)

    # ✅ Sekarang baru rename karena jumlah kolom sudah PASTI 8
    combined_df.columns = [
        "sector", "jan", "feb", "mar", "apr", "may", "kode_sektor"
    ]

    combined_df.reset_index(drop=True, inplace=True)
    combined_df.insert(0, "no", combined_df.index + 1)


    # Simpan ke Parquet
    parquet_io = BytesIO()
    combined_df.to_parquet(parquet_io, index=False)
    parquet_io.seek(0)
    return parquet_io



def upload_parquet_to_minio(parquet_io, date_str):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    parquet_name = "combined_kredit_sektoral.parquet"
    parquet_path = f"processed/{datetime.today().strftime('%Y-%m-%d')}/{parquet_name}"

    print(f"📤 Uploading Parquet to MinIO: {parquet_path}")
    client.put_object(
        MINIO_BUCKET,
        parquet_path,
        parquet_io,
        length=-1,
        part_size=10*1024*1024,
        content_type="application/octet-stream"
    )
    print("✅ Parquet upload completed")

if __name__ == "__main__":
    try:
        today_str = datetime.today().strftime("%Y%m%d")
        excel_data = download_excel_from_minio(today_str)
        parquet_data = transform_and_merge_sheets(excel_data)
        upload_parquet_to_minio(parquet_data, today_str)
    except Exception as e:
        print(f"🚨 Error during transformation: {e}")


