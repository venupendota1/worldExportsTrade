# 🌍 World Exports Trade Pipeline

An end-to-end data engineering pipeline for analyzing global export trade data using Azure Databricks, Delta Lake, and Apache Airflow.

---

## 🏗️ Architecture

Raw Data (CSV) → Bronze (Delta) → Silver (Delta) → Gold (Delta) → Dashboard

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Azure Databricks | Data processing & transformation |
| Delta Lake | Storage format (Bronze/Silver/Gold) |
| Azure Data Lake Storage Gen2 | Cloud storage |
| Apache Airflow (Docker) | Pipeline orchestration |
| PySpark | Data transformation |
| Databricks Dashboard | Dashboard & visualization |

---

## 📁 Project Structure
```
worldExportsTrade/
├── dags/                  # Airflow DAGs
├── notebooks/
│   ├── 00_config.py       # Spark config & mounts
│   ├── 01_bronze.py       # Raw ingestion
│   ├── 02_silver.py       # Cleaning & transformation
│   └── 03_gold.py         # Aggregation for dashboard
├── data/                  # Sample datasets
└── README.md
```

---

## ⚙️ Setup & Installation

### Prerequisites
- Docker Desktop
- Azure Account
- Databricks Workspace

### 1. Clone the repo
```bash
git clone https://github.com/venupendota1/worldExportsTrade.git
cd worldExportsTrade
```

### 2. Start Airflow
```bash
docker-compose up -d
```

### 3. Configure Connections in Airflow UI (localhost:8080)
- `databricks_default` → Databricks connection
- `azure_data_lake_default` → ADLS Gen2 connection

---

## 📊 Data Layers

- **Bronze** — Raw data ingested as-is
- **Silver** — Cleaned, typed, and enriched data
- **Gold** — Aggregated metrics for reporting

---

## 👤 Author
**Venu Pendota**  
[GitHub](https://github.com/venupendota1)
