from sqlalchemy import create_engine, text
import psycopg2

# PostgreSQL config
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_DB = "airflow"

def connect_engine():
    return create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

def insert_from_fact_raw_to_clean(conn):
    print("⏳ Loading data from raw → clean...")
    insert_sql = """
    INSERT INTO dm_fact_credit_growth_clean (
        no, sector,
        jan, feb, mar, apr, may, mom_feb, 
        mom_mar, mom_apr, mom_may, date, is_abnormal
    )
    SELECT 
        no, sector,
        jan, feb, mar, apr, may, mom_feb, 
        mom_mar, mom_apr, mom_may, date, is_abnormal
    FROM fact_credit_growth_clean;
    """
    conn.execute(text(insert_sql))
    print("✅ Loaded data from fact_credit_growth_clean to dm_fact_credit_growth_clean.")

def run_cleanup():
    engine = connect_engine()
    with engine.connect() as conn:
        print("✅ Connected to PostgreSQL")

        # Truncate table dm_fact_credit_growth
        truncate_sql = """
        TRUNCATE dm_fact_credit_growth_clean;
        """
        conn.execute(text(truncate_sql))

        # Copy from raw to clean
        insert_from_fact_raw_to_clean(conn)

        # DELETE no tertentu
        delete_sql = """
        DELETE FROM dm_fact_credit_growth_clean WHERE no IN (1, 26, 51, 76, 101, 126);
        """
        conn.execute(text(delete_sql))

        # UPDATE bank_type berdasarkan range no
        update_sql = """
        UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Persero' WHERE no BETWEEN 2 AND 25;
        UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Pemerintah Daerah' WHERE no BETWEEN 27 AND 50;
        UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Swasta Nasional' WHERE no BETWEEN 52 AND 75;
        UPDATE dm_fact_credit_growth_clean SET bank_type = 'Kantor Cabang di Luar Negeri' WHERE no BETWEEN 77 AND 100;
        UPDATE dm_fact_credit_growth_clean SET bank_type = 'Bank Perkreditan Rakyat' WHERE no BETWEEN 102 AND 125;
        UPDATE dm_fact_credit_growth_clean SET bank_type = 'Jumlah Semua Bank' WHERE no BETWEEN 127 AND 150;
        """
        conn.execute(text(update_sql))

        # UPDATE kode_sektor dari dim_sektor (via join)
        join_update_sql = """
        UPDATE dm_fact_credit_growth_clean f
        SET kode_sektor = d.kode_sektor
        FROM dim_sektor d
        WHERE f.sector = d.sector_name;
        """
        conn.execute(text(join_update_sql))

        # UPDATE kode_bank dari dim_bank_type (via join)
        join_update_sql = """
        UPDATE dm_fact_credit_growth_clean f
        SET kode_bank = d.bank_type
        FROM dim_bank_type d
        WHERE f.bank_type = d.bank_category;
        """
        conn.execute(text(join_update_sql))

        print("✅ Data cleanup and update completed.")

if __name__ == "__main__":
    try:
        run_cleanup()
    except Exception as e:
        print(f"🚨 Error during cleanup: {e}")