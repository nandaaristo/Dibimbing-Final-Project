# Automated Detection of Sectoral Credit Growth for Banking Risk Management

## Overview
This project implements an end-to-end data engineering pipeline to detect and monitor rapid credit growth across economic sectors in Indonesia, using publicly available data from Bank Indonesia (BI). It supports banking risk management by identifying abnormal credit surges and risk concentration patterns through automated processing and visualization.

The solution consists of ETL processes orchestrated with Apache Airflow, data stored in PostgreSQL, raw and processed files in MinIO (data lake), and interactive dashboards with Metabase. It includes anomaly detection logic and sectoral risk flagging to assist financial analysts and regulators in proactive decision-making.

---

## 📁 Project Structure
```
project_root/
├── dags/
│   ├── dag_credit_pipeline.py                  # Apache Airflow DAG for scheduling the pipeline
│   ├── scripts/
│   │   ├── scrape_bi_excel.py                  # Scrape Excel from BI and upload to MinIO
│   │   ├── transform_kredit_data.py            # Read from MinIO and normalize 3 Excel sheets
│   │   ├── transform_clean_fact_table.py       # Insert data from staging to Data Mart and clean the data
│   │   ├── transform_mom_flag.py               # Calculate MoM and flag sectoral risk and upload to MinIO
│   │   └── load_to_postgres.py                 # Load transformed results to PostgreSQL
│   └── sql/
│       └── create_fact_credit_growth.sql       # Table schema for fact_credit_growth
│       └── create_fact_credit_growth_clean.sql # Table schema for dm_fact_credit_growth_clean
│       └── create_fact_credit_growth_long.sql  # Table schema for dm_fact_credit_growth_long
│       └── create_table_dim_bank_type.sql      # Table schema for dim_bank_type
│       └── create_table_dim_sektor.sql         # Table schema for dim_sektor
├── data/                                       # (Optional local folder for testing)
├── logs/                                       # logs
├── docker-compose.yml                          # Multi-service environment
├── Dockerfile.airflow                          # Airflow Environment
├── requirements.txt                            # Python dependencies for ETL
├── README.md                                   # Project documentation
```

---

## 🎯 Objectives
- Unify credit data across sectors and bank classifications
- Calculate credit growth month-over-month
- Flag risk if sector growth exceeds 30%
- Visualize insights through dashboards
- Store raw and processed data in a structured MinIO data lake

---

## 📚 Data Source
- Bank Indonesia: Statistik Ekonomi dan Keuangan Indonesia (SEKI)
- File: Posisi Pinjaman/Kredit Rupiah yang Diberikan Bank Umum dan BPR Menurut Kelompok Bank & Lapangan Usaha (TABEL1_5.xls)
- Sheets Used: I.5_1, I.5_2, I.5_3

---

## Features
- 🌐 **Automated scraping** of Excel data from BI’s SEKI website
- ☁️ **MinIO storage** for raw Excel and processed Parquet files (data lake)
- 🧾 **ETL pipeline** to transform Excel into normalized format, compute MoM growth, and detect anomalies
- 🗃️ **PostgreSQL data warehouse** to store sectoral credit statistics and risk indicators
- 📊 **Interactive Metabase dashboards** showing sectoral trends, credit spikes, and concentration risks
- 🧠 **Risk flagging logic** based on thresholds (e.g., credit growth > 30%)
- 🔁 **Apache Airflow DAG** for scheduled and reproducible ETL workflows
- 🐳 **Docker Compose** to orchestrate MinIO, PostgreSQL, Airflow, and Metabase

---

## Stack
| Component         | Tool                        |
|-------------------|-----------------------------|
| Orchestration     | Apache Airflow              |
| Transformation    | pandas, openpyxl            |
| Data Lake         | MinIO                       |
| Storage           | PostgreSQL                  |
| Dashboard         | Metabase                    |
| Containerization  | Docker, docker-compose      |

---

## Architecture
```
+----------------------------+
| BI SEKI Website (Excel)   |
+-------------+--------------+
              |
              v
+----------------------------+
| Scrape & Upload to MinIO  |
| - Store raw Excel files   |
+-------------+--------------+
              |
              v
+----------------------------+
| Airflow DAG Scheduler     |
| - Transform & Clean       |
| - Calculate MoM Growth    |
| - Flag Risk Anomalies     |
+-------------+--------------+
              |
              v
+------------------------------+
| PostgreSQL Data Warehouse    |
| - dm_fact_credit_growth_long |
+-------------+----------------+
              |
              v
+----------------------------+
| Metabase Dashboard        |
| - Sectoral Credit Trends  |
| - MoM Anomaly Detection   |
| - Credit Concentration    |
+----------------------------+
```

---

## Getting Started
1. Clone this repository
2. Run `docker compose up -d` to launch services (MinIO, PostgreSQL, Airflow, Metabase)
3. Generate Fernet key and secret key for Airflow and update `docker-compose.yml`
4. Install Python dependencies in Airflow using `requirements.txt`
5. Place your DAG and scripts in `dags/`
6. Access tools:
   - MinIO: http://localhost:9001 (admin/admin123)
   - Airflow: http://localhost:8080
   - Metabase: http://localhost:3000

---

## Author
Project by **Fernanda Aristo Abimanyu** — developed for use case demonstration and learning in financial data engineering.

---

## License
This project is for educational and portfolio purposes only. Refer to Bank Indonesia's data usage policy before deploying publicly.
