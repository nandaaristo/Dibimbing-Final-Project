import pandas as pd
import os

# Path input dan output
EXCEL_PATH = "/Users/arethusaaryandhana/Nanda/Dibimbing - Data Engineer Batch 10/## PROJECT ##/data/raw/TABEL1_5.xls"
OUTPUT_PATH = "/Users/arethusaaryandhana/Nanda/Dibimbing - Data Engineer Batch 10/## PROJECT ##/data/output/combined_kredit_sektoral.xlsx"

# Sheet dan kategori yang akan digabung
SHEET_MAP = {
    "I.5_1": "Kelompok Bank",
    "I.5_2": "Jenis Bank",
    "I.5_3": "Jenis Kredit"
}

def combine_sheets_raw(excel_path, output_path):
    combined_df = pd.DataFrame()

    for sheet_name, kategori in SHEET_MAP.items():
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None, dtype=str)
        df.insert(0, "kategori", kategori)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined_df.to_excel(output_path, index=False, header=False)
    print(f"✅ Gabungan sheet berhasil disimpan di: {output_path}")

if __name__ == "__main__":
    combine_sheets_raw(EXCEL_PATH, OUTPUT_PATH)
