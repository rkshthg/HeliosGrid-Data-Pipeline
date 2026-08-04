# ☀️ Project HeliosGrid: Solarpunk Renewable Energy Lakehouse Platform

[![Databricks](https://img.shields.io/badge/Databricks-AWS-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion-00ADD8?logo=apachespark&logoColor=white)](https://delta.io/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-003366)](https://docs.databricks.com/data-governance/unity-catalog/index.html)
[![AWS S3](https://img.shields.io/badge/AWS-S3_%26_Lambda-569A31?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)

**Project HeliosGrid** is a cloud-native **Databricks Lakehouse Data Platform** engineered on the **Medallion Architecture (Bronze, Silver, Gold)** and governed by **Unity Catalog**. 

It continuously ingests high-frequency solar radiation metrics (GHI, DNI, DHI), weather telemetry, and atmospheric aerosol data across **4 major Indian solar microclimates** to power real-time grid stabilization, predictive yield analytics, and thermal efficiency modeling for India’s National Solar Mission (500 GW target by 2030).

---

## 🌏 Project Purpose & Real-World Impact

India is rapidly expanding its solar footprint toward 500 GW of non-fossil energy capacity by 2030. However, integrating massive solar parks, such as **Bhadla Solar Park** (2.25 GW in Rajasthan) and **Pavagada Solar Park** (2.05 GW in Karnataka), into the national electrical grid presents severe grid stability challenges due to atmospheric physics unique to the Indian subcontinent:

1. **Extreme Summer Overheating (Thermal Derating)**: Silicon photovoltaic (PV) panel operating temperatures frequently exceed $45^\circ\text{C}$ in Rajasthan and Central India, triggering thermal efficiency losses of **10%–15%** during peak sun hours.
2. **Monsoonal Volatility**: Sudden cloud bursts during the Southwest Monsoon (June–Sept) and Northeast Monsoon (Oct–Dec) cause rapid generation drops that strain grid frequency control.
3. **Aerosol & Dust Attenuation**: High Aerosol Optical Depth (AOD), desert dust soiling, and severe winter smog ($PM_{2.5}$ / $PM_{10}$) in the Gangetic plains attenuate Direct Normal Irradiance (DNI).

### 🎯 How HeliosGrid Solves This:
HeliosGrid bridges environmental physics and high-performance data engineering. By deriving real-time **PV panel cell temperatures ($T_{\text{cell}}$)**, **thermal derating factors ($\eta_{\text{thermal}}$)**, and **monsoonal seasonality tags**, HeliosGrid empowers national load dispatch centers (SLDCs) to forecast solar generation drop-offs and optimize battery energy storage system (BESS) dispatch.

---

## 🏛️ End-to-End Architecture Overview

```
+-----------------------------------------------------------------------------------+
|                        HELIOSGRID MEDALLION LAKEHOUSE                             |
|                                                                                   |
|  [AWS Lambda / Harvester Script]  --->  [AWS S3 / UC Volume Landing Zone]         |
|                                                      │                            |
|                                                      ▼                            |
|  [Databricks Auto Loader cloudFiles]  --->  [Bronze Raw Delta Tables]             |
|                                                      │                            |
|                                                      ▼                            |
|  [PySpark Physics & Weather Engine]   --->  [Silver Cleansed Time-Series Table]   |
|                                                      │                            |
|                                                      ▼                            |
|  [Delta Lake MERGE INTO Upserts]     --->  [Gold Kimball Star Schema Marts]       |
|                                                      │                            |
|                                                      ▼                            |
|  [Databricks Workflows Automation]   --->  [Multi-Task Scheduled DAG Job]          |
+-----------------------------------------------------------------------------------+
```

---

## 🛠️ Technology Stack & Architectural Trade-offs

| Technology | Role in Platform | Architectural Rationale & Why Chosen over Alternatives |
|---|---|---|
| **Databricks on AWS** | Core Compute Engine | Selected for unified analytics over cloud storage. Provides serverless PySpark execution, Unity Catalog governance, and native Delta Lake ACID logs. |
| **Delta Lake** | Storage Format | Chosen over proprietary warehouses (Snowflake/BigQuery) to retain 100% open Parquet data ownership in AWS S3, eliminating vendor lock-in and double storage costs. |
| **Auto Loader (`cloudFiles`)** | Bronze Ingestion | Chosen over Kafka or standard directory listing (`ls`). Provides cloud-native file streaming with automated schema evolution (`addNewColumns`), checkpointing, and corrupt field rescue (`_rescued_data`) at zero Kafka cluster overhead. |
| **PySpark** | Silver Cleansing & Physics Engine | Selected over dbt/SQL for complex non-linear physics matrix operations (silicon PV thermal derating equations, array zipping/exploding via `arrays_zip` + `explode`). |
| **Kimball Star Schema & MERGE INTO** | Gold Data Marts | Implemented in Gold to provide sub-second query performance for Power BI/SQL BI dashboards via ACID upserts (`MERGE INTO`). |
| **Delta Z-Ordering** | Performance Tuning | Applied on `(record_timestamp, location_id)` to group related records physically within Parquet files, allowing Spark to perform **95%+ Data Skipping** without B-Tree indexes. |
| **Databricks Workflows** | Pipeline Orchestration | Selected over Apache Airflow/Cloud Composer. Offers native multi-task DAG job orchestration on **ephemeral Job Clusters**, saving up to **50% in compute cost**. |
| **AWS Lambda & EventBridge** | Serverless Harvester | Runs the 5-second REST API harvester on a scheduled cron for **$0.00/month** infrastructure cost, avoiding keeping interactive clusters running 24/7. |

---

## 📊 Medallion Pipeline Layers

### 📁 1. Landing Zone (AWS S3 & Unity Catalog Volume)
- **Location**: `/Volumes/heliosgrid_catalog/bronze/landing_raw_json/`
- **Function**: Python Harvester script / AWS Lambda fetches hourly Open-Meteo REST API payloads for 4 Indian stations (Bhadla, Pavagada, Charanka, Delhi NCR) and streams JSON files to AWS S3 via `boto3`.

### 📊 2. Bronze Layer (Raw Delta Tables)
- **Tables**: `heliosgrid_catalog.bronze.raw_forecast_telemetry`, `raw_air_quality_telemetry`
- **Function**: Ingests raw landed JSON files 1:1 using Auto Loader (`cloudFiles`). Preserves raw structure and tracks ingestion lineage (`_source_file`, `_ingested_timestamp`).

### 🧹 3. Silver Layer (Cleansed Time-Series Table)
- **Table**: `heliosgrid_catalog.silver.cleaned_solar_telemetry`
- **Function**:
  - Parses JSON strings via `from_json()`.
  - Unnests 72-hour parallel arrays into individual hourly rows using `F.arrays_zip()` and `F.explode()`.
  - Enforces strict data types (Timestamp, Double).
  - Derives PV Cell Temperature: $T_{\text{cell}} = T_{\text{ambient}} + \left(\frac{25}{800}\right) \times \text{GHI}$.
  - Derives Thermal Derating Factor: $\eta_{\text{thermal}} = 1.0 - 0.004 \times (T_{\text{cell}} - 25)$ for $T_{\text{cell}} > 25^\circ\text{C}$.
  - Tags Indian monsoon phases (`Pre-Monsoon`, `Southwest Monsoon`, `Northeast Monsoon`, `Winter`).
  - Optimized via `OPTIMIZE ... ZORDER BY (record_timestamp, station_id)`.

### 📐 4. Gold Layer (Kimball Dimensional Star Schema)
- **Tables**:
  - `heliosgrid_catalog.gold.dim_location` (Spatial Dimension)
  - `heliosgrid_catalog.gold.dim_time` (Temporal & Seasonality Dimension)
  - `heliosgrid_catalog.gold.dim_weather_condition` (Environmental & Air Quality Dimension)
  - `heliosgrid_catalog.gold.fact_solar_generation` (Fact Table)
- **Function**: Serves curated BI data. Deduplicates source DataFrames (`.dropDuplicates(["location_id", "record_timestamp"])`) and executes Delta Lake ACID `MERGE INTO` upserts. Includes calculated measures like `expected_power_kw` (for a 100 MW solar plant) and `effective_irradiance_wm2`.

---

## 📈 Sample Gold Analytics SQL Queries

### Query 1: Expected Solar Yield (MWh) by Indian Park & Thermal Efficiency
```sql
SELECT 
    l.station_name,
    l.state_name,
    l.climate_zone,
    ROUND(SUM(f.expected_power_kw) / 1000.0, 2) AS total_expected_mwh,
    ROUND(AVG(f.thermal_derating_factor) * 100, 2) AS avg_thermal_efficiency_pct
FROM heliosgrid_catalog.gold.fact_solar_generation f
JOIN heliosgrid_catalog.gold.dim_location l ON f.location_id = l.location_id
GROUP BY l.station_name, l.state_name, l.climate_zone
ORDER BY total_expected_mwh DESC;
```

### Query 2: Monsoon Volatility & Overheating Loss Analysis
```sql
SELECT 
    t.monsoon_season,
    ROUND(AVG(f.ghi_wm2), 2) AS avg_ghi_wm2,
    ROUND(AVG(f.cell_temp_celsius), 2) AS avg_cell_temp_celsius,
    ROUND(AVG(f.thermal_derating_factor) * 100, 2) AS avg_thermal_efficiency_pct,
    ROUND(SUM(f.expected_power_kw) / 1000.0, 2) AS total_mwh
FROM heliosgrid_catalog.gold.fact_solar_generation f
JOIN heliosgrid_catalog.gold.dim_time t ON f.time_id = t.time_id
GROUP BY t.monsoon_season
ORDER BY total_mwh DESC;
```

---

## 📁 Repository Structure

```
HeliosGrid/
├── data/                               # Local testing storage directory
├── harvester.py                        # Python API Harvester (Open-Meteo REST -> AWS S3)
├── 01_bronze_ingestion.ipynb           # Auto Loader Streaming Notebook (Bronze Layer)
├── 02_silver_transform.ipynb           # PySpark Cleansing & Physics Notebook (Silver Layer)
├── 03_gold_load.ipynb                  # Kimball Star Schema & Delta MERGE Notebook (Gold Layer)
├── .env                                # Environment variable configurations
└── README.md                           # Platform Documentation & Overview
```

---

## 🚀 Getting Started

### Prerequisites
- Databricks Workspace on AWS (with Unity Catalog enabled).
- AWS IAM Role (`heliosgrid-role-1`) attached to Databricks Storage Credential.
- Python 3.10+ with `boto3`, `requests`, and `python-dotenv`.

### Quick Run Guide
1. **Fetch Telemetry**: Run `python harvester.py` locally or trigger AWS Lambda to land fresh JSON telemetry files into S3.
2. **Execute Medallion Pipeline**:
   - Run `01_bronze_ingestion.ipynb` to stream landed JSONs into Bronze Delta tables.
   - Run `02_silver_transform.ipynb` to unnest arrays, cast types, and apply thermal derating formulas.
   - Run `03_gold_load.ipynb` to update the Gold Star Schema via Delta `MERGE INTO`.
3. **Automate Pipeline**: Open **Databricks Workflows**, create job `HeliosGrid_National_Solar_Pipeline`, chain Tasks 1 $\to$ 2 $\to$ 3 $\to$ 4, and attach an hourly schedule on ephemeral Job Clusters!

---

- **Domain Focus**: Renewable Energy Lakehouses, Databricks Unity Catalog, PySpark Atmospheric Physics Engine.
