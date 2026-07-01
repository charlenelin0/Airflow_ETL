# Weather Data Platform

使用 Airflow、MinIO、PostgreSQL、BigQuery、dbt 與 Databricks 建置的天氣資料平台。

---

## 簡介

本專案實作一條天氣資料 ELT Pipeline，並新增一條 Databricks 平台內 ETL parallel track。

目前資料來源為：

- [Open-Meteo Forecast API](https://open-meteo.com/)
- BigQuery `weather_platform.dim_city`：提供啟用城市的 `country`、`latitude`、`longitude`

目前擷取的 weather variables：

- `temperature_2m`
- `rain`

主要流程：

- Airflow 依據 BigQuery `dim_city` 取得城市座標
- 透過 Open-Meteo API 擷取指定日期的 hourly weather data
- Raw JSON 同步寫入 MinIO bronze 與 Databricks Volume bronze landing
- MinIO path 進行既有 local transform、Postgres staging、BigQuery loading 與 dbt modeling
- Databricks path 在平台內完成 bronze-to-silver 與 silver-to-gold

---

## 技術架構

| Category | Technology |
| --- | --- |
| Workflow Orchestration | Apache Airflow |
| Programming Language | Python |
| Data Lake | MinIO |
| Staging Warehouse | PostgreSQL |
| Cloud Warehouse / City Source | BigQuery |
| Lakehouse Transform Platform | Databricks |
| Data Transformation & Modeling | dbt |
| Containerization | Docker |
| Notification | SMTP Email |

---

## 系統架構

```text
                  Apache Airflow
                         │
                         ▼
        BigQuery dim_city active cities
                         │
                         ▼
                 Open-Meteo API
                         │
             ┌───────────┴───────────┐
             ▼                       ▼
   MinIO Bronze Raw JSON     Databricks Volume Raw JSON
             │                       │
             ▼                       ▼
   MinIO Silver Parquet      Databricks Silver Delta
             │                       │
             ▼                       ▼
   PostgreSQL Staging        Databricks Gold Delta
             │
             ▼
      BigQuery Staging
             │
             ▼
         dbt Models
             │
             ▼
      Facts / Dimensions
```

---

## Airflow / MinIO / dbt Flow

Airflow DAG：

```text
weather_data_pipeline
```

主要 task：

| Step | Task / Function | Description |
| --- | --- | --- |
| Pipeline state | `update_pipeline_running()` | 標記 pipeline running |
| Extract | `scrape_weatherforecast()` | 讀取 BigQuery city list，呼叫 Open-Meteo API |
| Bronze | `upload_json_to_minio()` | raw JSON 寫入 MinIO bronze |
| Databricks landing | `upload_json_to_databricks_volume()` | raw JSON 寫入 Databricks Volume |
| Silver | `trans_minio_silver_forecast()` | flatten hourly JSON 並寫入 MinIO silver parquet |
| Staging | `ins_postgres_staging_forecast()` | 載入 PostgreSQL staging |
| BigQuery | `ins_bigquery_staging_forecast()` | 載入 BigQuery staging |
| Modeling | `run_dbt()` | 執行 dbt models |
| Pipeline state | `update_pipeline_success()` | 更新 row count 與成功狀態 |

MinIO bronze layout：

```text
bronze/weather/{temperature|rain}/date=YYYY-MM-DD/country=<country>.json
```

MinIO silver output：

```text
weather/etl_temperature_YYYY-MM-DD.parquet
weather/etl_rain_YYYY-MM-DD.parquet
```

---

## Databricks Platform ETL

Databricks 建立獨立 ETL pipeline. 只負責 raw JSON landing，不負責觸發 Databricks Workflow。

Databricks notebook 來源透過 Databricks Git Folder 連接 GitHub repository，並在 Databricks UI 中維護 Workflow。

平台內 ETL 流程：

| Layer | Notebook | Description |
| --- | --- | --- |
| Bronze to Silver | `databricks/notebooks/weather_bronze_to_silver.py` | 讀取 Volume raw JSON，將 hourly nested data 展平成 silver Delta tables |
| Silver to Gold | `databricks/notebooks/weather_silver_to_gold.sql` | 使用 Databricks SQL 將 silver tables 彙總為 gold daily summary |

Databricks outputs：

```text
silver.weather_temperature_hourly
silver.weather_rain_hourly
gold.weather_daily_summary
```

---

## dbt 建模

既有 warehouse path 由 dbt 進行資料轉換與建模。

### Staging Layer

- `stg_weather_forecast`
- `stg_weather_alert`

### Intermediate Layer

- `eph_weather_forecast`
- `eph_weather_alert`

### Mart Layer

Dimension tables：

- `dim_city`
- `dim_area`

Fact tables：

- `fact_weather_forecast`
- `fact_weather_alert`

---

## Data Governance & Documentation

透過 dbt metadata、PostgreSQL metadata 與 repo evidence 建立資料治理文件。

內容包含：

- Source definitions
- Model descriptions
- Column-level documentation
- Data lineage
- Data quality tests
- In-scope table / view / column inventory

治理文件：

```text
docs/weather_db_governance.md
```

---

## Pipeline 狀態監控與追蹤

Pipeline state table 用於記錄執行狀態與處理結果。

| 欄位 | 說明 |
| --- | --- |
| `pipeline_name` | Pipeline 名稱 |
| `status` | RUNNING / SUCCESS / FAILED |
| `row_count` | 本次處理筆數 |
| `last_processed_at` | 本次處理時間 |
| `last_run_at` | 最近一次執行時間 |
| `created_at` | Pipeline 建立時間 |
| `updated_at` | Pipeline 狀態最後更新時間 |

---

## 失敗通知機制

當 Airflow task 執行失敗時會寄送 email notification。

設定：

```python
@task(on_failure_callback=[failure_email])
```

通知內容包含：

- DAG name
- Task name
- Run ID
- Log URL

---

## 專案結構

```text
├── dags
│   ├── weather_data_pipeline.py
│   └── include
│       ├── api
│       ├── databricks
│       ├── storage
│       └── transform
├── databricks
│   ├── notebooks
│   │   ├── weather_bronze_to_silver.py
│   │   └── weather_silver_to_gold.sql
│   └── README.md
├── dbt
│   └── weather_data_platform
│       ├── dbt_project.yml
│       ├── macros
│       └── models
├── docs
└── docker-compose.yaml
```

---

## 執行方式

### 啟動 Airflow

```bash
docker compose up -d
```

Airflow UI：

```text
http://localhost:8080
```

### 觸發 Airflow Pipeline

手動執行 DAG：

```text
weather_data_pipeline
```

### 執行 Databricks ETL

在 Databricks UI 建立或執行 Workflow：

```text
weather_bronze_to_silver.py
  -> weather_silver_to_gold.sql
```

設定 Workflow parameter：

```text
batch_date = YYYY-MM-DD
```

### 執行 dbt

```bash
cd dbt/weather_data_platform
dbt run
```

### 產生 dbt 文件

```bash
dbt docs generate
dbt docs serve
```