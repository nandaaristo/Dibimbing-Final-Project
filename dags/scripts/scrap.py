import os
import requests
from datetime import datetime

EXCEL_URL = "https://www.bi.go.id/SEKI/tabel/TABEL1_5.xls"
DOWNLOAD_FOLDER = "data/raw"

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def download_excel(url):
    filename = f"TABEL1_5_{datetime.today().strftime('%Y%m%d')}.xls"
    filepath = os.path.join(DOWNLOAD_FOLDER, filename)
    print(f"⬇️  Mengunduh file Excel dari {url}...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"✅ File berhasil disimpan di: {filepath}")
        return filepath
    else:
        raise Exception(f"❌ Gagal mengunduh file. Status code: {response.status_code}")

if __name__ == "__main__":
    try:
        file_path = download_excel(EXCEL_URL)
    except Exception as e:
        print(f"🚨 Terjadi kesalahan: {e}")
