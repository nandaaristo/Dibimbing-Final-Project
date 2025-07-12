import pandas as pd
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

def transform_excel_minio_to_parquet():
    try:
        # === Step 1: Download Excel dari MinIO ===
        date_str = datetime.today().strftime('%Y%m%d')
        date_folder = datetime.today().strftime('%Y-%m-%d')
        excel_filename = f"TABEL1_5_{date_str}.xls"
        object_path = f"raw/{date_folder}/{excel_filename}"

        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        print(f"⬇️ Downloading Excel from MinIO: {object_path}")
        response = client.get_object(MINIO_BUCKET, object_path)
        excel_io = BytesIO(response.read())

        # === Step 2: Transform dan gabungkan sheet ===
        combined_df = pd.DataFrame()

        for sheet_name in SHEET_LIST:
            df_raw = pd.read_excel(excel_io, sheet_name=sheet_name, header=None, dtype=str)
            df_clean = df_raw.iloc[5:55]  # Ambil baris ke-6 sampai 55
            df_clean = df_clean.iloc[:, [2, 3, 4, 5, 6, 7, 8]]  # Ambil kolom yang diperlukan
            combined_df = pd.concat([combined_df, df_clean], ignore_index=True)

        # === Step 3: Rename kolom dan tambahkan no ===
        combined_df.columns = ["sector", "jan", "feb", "mar", "apr", "may", "kode_sektor"]
        combined_df.reset_index(drop=True, inplace=True)
        combined_df.insert(0, "no", combined_df.index + 1)

        # === Step 4: Upload kembali ke MinIO dalam format Parquet ===
        parquet_io = BytesIO()
        combined_df.to_parquet(parquet_io, index=False)
        parquet_io.seek(0)

        parquet_name = "combined_kredit_sektoral.parquet"
        parquet_path = f"processed/{date_folder}/{parquet_name}"

        print(f"📤 Uploading Parquet to MinIO: {parquet_path}")
        client.put_object(
            MINIO_BUCKET,
            parquet_path,
            parquet_io,
            length=-1,
            part_size=10 * 1024 * 1024,
            content_type="application/octet-stream"
        )
        print("✅ Parquet upload completed")

    except Exception as e:
        print(f"🚨 Error in transform_excel_minio_to_parquet: {e}")
