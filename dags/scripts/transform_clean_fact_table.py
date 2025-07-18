from sqlalchemy import create_engine, text
import psycopg2
import pandas as pd

# PostgreSQL config
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_DB = "postgres"

def cleanup_and_load_fact_clean():
    try:
        # 🔌 Connect engine
        engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

        # ✅ Autocommit mode
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            print("✅ Connected to PostgreSQL")

            # 🧹 DELETE berdasarkan tanggal hari ini
            print("🧹 Deleting existing rows for today...")
            conn.execute(text("DELETE FROM dm_fact_credit_growth_clean WHERE date = CURRENT_DATE;"))

            # 📥 INSERT from source
            print("⏳ Inserting from fact_credit_growth...")
            conn.execute(text("""
                INSERT INTO dm_fact_credit_growth_clean (
                    no, sector, jan, feb, mar, apr, may,
                    mom_feb, mom_mar, mom_apr, mom_may,
                    date, is_abnormal
                )
                SELECT
                    no, sector, jan, feb, mar, apr, may,
                    mom_feb, mom_mar, mom_apr, mom_may,
                    date, is_abnormal
                FROM fact_credit_growth;
            """))

            # ❌ DELETE specific rows
            print("🧽 Deleting unneeded rows...")
            conn.execute(text("DELETE FROM dm_fact_credit_growth_clean WHERE no IN (1, 26, 51, 76, 101, 126);"))

            # 🏦 Update bank_type berdasarkan range
            print("📝 Updating bank_type by range...")
            conn.execute(text("""
                UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Persero' WHERE no BETWEEN 2 AND 25;
                UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Pemerintah Daerah' WHERE no BETWEEN 27 AND 50;
                UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Swasta Nasional' WHERE no BETWEEN 52 AND 75;
                UPDATE dm_fact_credit_growth_clean SET bank_type = 'Kantor Cabang di Luar Negeri' WHERE no BETWEEN 77 AND 100;
                UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Perkreditan Rakyat' WHERE no BETWEEN 102 AND 125;
                UPDATE dm_fact_credit_growth_clean SET bank_type = 'Jumlah Semua Bank' WHERE no BETWEEN 127 AND 150;
            """))

            # 🧩 Update kode_sektor dan kode_bank via join
            print("🔗 Updating kode_sektor from dim_sektor...")
            conn.execute(text("""
                UPDATE dm_fact_credit_growth_clean f
                SET kode_sektor = d.kode_sektor
                FROM dim_sektor d
                WHERE f.sector = d.sector_name;
            """))

            print("🔗 Updating kode_bank from dim_bank_type...")
            conn.execute(text("""
                UPDATE dm_fact_credit_growth_clean f
                SET kode_bank = d.bank_type
                FROM dim_bank_type d
                WHERE f.bank_type = d.bank_category;
            """))

            print("✅ Data cleanup and load completed.")

            # 🔁 Transform to long format and insert to new table
            print("🔁 Transforming to long format...")

            df_wide = pd.read_sql("SELECT * FROM dm_fact_credit_growth_clean WHERE date = CURRENT_DATE", conn)

            value_vars = ["jan", "feb", "mar", "apr", "may"]
            mom_vars = ["mom_feb", "mom_mar", "mom_apr", "mom_may"]

            # Mapping untuk mom column dari month
            mom_lookup = {
                "feb": "mom_feb",
                "mar": "mom_mar",
                "apr": "mom_apr",
                "may": "mom_may"
            }

            # Buat dataframe mom
            df_mom = pd.melt(
                df_wide,
                id_vars=["kode_sektor", "sector", "bank_type", "kode_bank", "date", "is_abnormal"],
                value_vars=["mom_feb", "mom_mar", "mom_apr", "mom_may"],
                var_name="mom_month",
                value_name="mom"
            )

            # Bersihkan nama bulan di kolom mom_month agar bisa di-join
            df_mom["month"] = df_mom["mom_month"].str.replace("mom_", "")

            # Transform wide → long
            df_long = pd.melt(
                df_wide,
                id_vars=["kode_sektor", "sector", "bank_type", "kode_bank", "date", "is_abnormal"],
                value_vars=["jan", "feb", "mar", "apr", "may"],
                var_name="month",
                value_name="credit_growth"
            )

            # Join nilai mom berdasarkan 'month' dan key lainnya
            df_long = df_long.merge(
                df_mom.drop(columns="mom_month"),
                on=["kode_sektor", "sector", "bank_type", "kode_bank", "date", "is_abnormal", "month"],
                how="left"
            )

             # 🧠 is_abnormal jika mom > 15%
            df_long["is_abnormal"] = df_long["mom"].gt(15)

            # Tambahkan no unik
            df_long.reset_index(drop=True, inplace=True)
            df_long["no"] = df_long.index + 1

            # Buang data hari ini di tabel long
            conn.execute(text("DELETE FROM dm_fact_credit_growth_long WHERE date = CURRENT_DATE"))

            # Simpan ke tabel long
            df_long.to_sql("dm_fact_credit_growth_long", engine, if_exists="append", index=False)
            print("✅ Inserted long format data to dm_fact_credit_growth_long")


    except Exception as e:
        print(f"🚨 Error in cleanup_and_load_fact_clean: {e}")

if __name__ == "__main__":
    cleanup_and_load_fact_clean()