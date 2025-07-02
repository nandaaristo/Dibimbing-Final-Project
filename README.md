
# Automated Detection of Sectoral Credit Growth for Banking Risk Management

## Overview
This project implements an end-to-end data engineering pipeline to detect and monitor rapid credit growth across economic sectors in Indonesia, using publicly available data from Bank Indonesia (BI). It supports banking risk management by identifying abnormal credit surges and risk concentration patterns through automated processing and visualization.

The solution consists of ETL processes orchestrated with Apache Airflow, data storage in PostgreSQL, and interactive dashboards with Metabase. It includes anomaly detection logic and sectoral risk flagging to assist financial analysts and regulators in proactive decision-making.

---

## 📁 Project Structure
```
project_root/
├── data/
│   ├── raw/                      # Contains manually downloaded Excel from BI
│   ├── processed/                # Output of merged Excel and transformed data
├── scripts/
│   ├── transform_credit_data.py  # Merge and normalize 3 Excel sheets
│   ├── analyze_credit_growth.py  # Calculate MoM and risk flagging
├── dags/
│   └── dag_credit_pipeline.py    # Apache Airflow DAG for scheduling the pipeline
├── dashboards/                   # Metabase chart configuration (optional)
├── README.md                     # Project documentation
```

---

## 🎯 Objectives
- Unify credit data across sectors and bank classifications
- Calculate credit growth month-over-month
- Flag risk if sector growth exceeds 30%
- Visualize insights through dashboards

---

## 📚 Data Source
- Bank Indonesia: Statistik Ekonomi dan Keuangan Indonesia (SEKI)
- File: Posisi Pinjaman/Kredit Rupiah yang Diberikan Bank Umum dan BPR Menurut Kelompok Bank & Lapangan Usaha (TABEL1_5.xls)
- Sheets Used: I.5_1, I.5_2, I.5_3

---

## Features
- 📥 **Manual Download** of Excel data from BI’s SEKI website
- 🧾 **ETL pipeline** to transform Excel into normalized format, compute MoM growth, and detect anomalies
- 🗃️ **PostgreSQL data warehouse** to store sectoral credit statistics and risk indicators
- 📊 **Interactive Metabase dashboards** showing sectoral trends, credit spikes, and concentration risks
- 🧠 **Risk flagging logic** based on thresholds (e.g., credit growth > 30%)
- 📤 **Apache Airflow DAG** for scheduled and reproducible ETL workflows
- 🐳 **Docker-based environment** for reproducibility and deployment

---

## Stack
| Component         | Tool                        |
|-------------------|-----------------------------|
| Orchestration     | Apache Airflow              |
| Transformation    | pandas, openpyxl            |
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
| Airflow DAG Scheduler     |
| - Transform & Clean       |
| - Calculate MoM Growth    |
| - Flag Risk Anomalies     |
+-------------+--------------+
              |
              v
+----------------------------+
| PostgreSQL Data Warehouse |
| - fact_credit_growth      |
| - dim_sector              |
+-------------+--------------+
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
1. Clone this repo
2. Set up environment with Docker Compose
3. Configure Airflow DAG and connections
4. Run the ETL pipeline
5. Connect Metabase to PostgreSQL and import dashboard templates

---

## Author
Project by **Fernanda Aristo Abimanyu** — developed for use case demonstration and learning in financial data engineering.

---

## License
This project is for educational and portfolio purposes only. Refer to Bank Indonesia's data usage policy before deploying publicly.
