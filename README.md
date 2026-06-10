# Weather Data Platform

使用 Airflow、MinIO、PostgreSQL 與 dbt 建置的天氣資料平台。

---

## 簡介

本專案使用 Apache Airflow、dbt、MinIO 與 PostgreSQL 建置 ELT Pipeline，實作資料擷取、轉換與建模流程。

資料處理流程如下：

- 透過 [中央氣象署開放資料平臺 API](https://opendata.cwa.gov.tw/dist/opendata-swagger.html) 定期擷取（Extract）天氣預報與高溫警報資料
- 將原始資料以 JSON 格式存放於 MinIO Data Lake Bronze Layer，保留完整來源資料供追溯與重跑使用
- 將巢狀 JSON 資料展平，萃取所需欄位並轉換為 Parquet 格式，存放於 MinIO Data Lake Silver Layer
- 將整理後的資料載入（Load）Data Warehouse
- 使用 dbt 實作資料建模（Data Modeling）與商業邏輯轉換（Transform），建立 Fact / Dimension Model 與 Data Mart，供後續資料分析與查詢使用
  
---

## 技術架構

| Category                       | Technology     |
| ------------------------------ | -------------- |
| Workflow Orchestration         | Apache Airflow |
| Programming Language           | Python         |
| Data Lake                      | MinIO          |
| Staging Data Warehouse         | PostgreSQL     |
| Cloud Data Warehouse           | BigQuery       |
| Data Transformation & Modeling | dbt            |
| Containerization               | Docker         |
| Notification                   | SMTP Email     |

---

## 系統架構

```text

                 Apache Airflow
                        │
                        ▼
                   Weather API
                        │
                        ▼
      Bronze Layer (Raw JSON in MinIO)
                        │
                        ▼
       Silver Layer (Parquet in MinIO)
                        │
                        ▼
   PostgreSQL Data Warehouse (Staging)
                        │
                        ▼
          BigQuery Data Warehouse
                        │
                        ▼
                 dbt Models
                        │
                        ▼
        Fact / Dimension Models
                        │
                        ▼
                   Data Mart
```

Data Sources:
- Weather Forecast
- High Temperature Alert

---

## 資料流程

### 1. Weather Forecast ETL

#### Extract

透過 Weather API 擷取天氣預報資料。

Airflow Task：

```python
scrape_weatherforecast()
```

取得資料內容：

- 地區名稱
- 最高溫
- 最低溫

---

#### Bronze Layer

原始 API 回傳資料存入 MinIO。

格式：

```json
weather_forecast.json
```

目的：

- 保留原始資料
- 支援資料追溯 / 重新處理

---

#### Silver Layer

進行資料清洗與轉換。

Airflow Task：

```python
trans_minio_silver_weather()
```

處理內容：

- JSON Flatten
- 欄位命名標準化

輸出：

```parquet
weather_forecast.parquet
```

---

#### Load to PostgreSQL

Airflow Task：

```python
ins_postgres_staging_forecast()
```

目標資料表：

```sql
staging_forecast
```

---

### 2. Weather Alert ETL

#### Extract

擷取高溫警報資料。

Airflow Task：

```python
scrape_hightempalert()
```

取得：

- 警報區域
- 警報等級
- 發布時間
- 生效時間

---

#### Transform

Airflow Task：

```python
trans_minio_silver_alert()
```

進行：

- 欄位整理
- 日期格式轉換
- 資料清洗

---

#### Load

Airflow Task：

```python
ins_postgres_staging_alert()
```

目標資料表：

```sql
staging_alert
```

---

## Airflow DAG

本專案採用 TaskFlow API 開發。

<img width="1257" height="841" alt="weather_data_pipeline-graph (2)" src="https://github.com/user-attachments/assets/8aae0d6c-ee65-4712-8e37-34c287508ba2" />

---

## dbt 建模

資料載入資料倉儲後，由 dbt 進行資料轉換與建模。

### Staging Layer (Views)：資料清洗與標準化

- stg_weather_forecast
- stg_weather_alert

### Intermediate Layer (Ephemeral Models)：商業邏輯轉換

- eph_weather_forecast
- eph_weather_alert

### Mart Layer (Tables)：資料倉儲模型，提供 BI 與資料分析使用

#### Dimension Tables

- dim_city
- dim_area

#### Fact Tables

- fact_weather_forecast
- fact_weather_alert

<img width="1759" height="784" alt="image" src="https://github.com/user-attachments/assets/92c1eee0-6221-4ce5-964b-520950d8164b" />

---

## Data Governance & Documentation 資料治理與文件化

透過 dbt Metadata 管理機制，建立資料來源定義、模型與欄位文件、資料血緣追蹤（Data Lineage）及資料品質驗證（Data Quality Tests），提升資料可維護性與治理能力。

包含：

- Source Definitions（資料來源定義）
- Model Descriptions（模型說明）
- Column-Level Documentation（欄位文件）
- Model Tags（模型標籤管理）
- Data Lineage Tracking（資料血緣追蹤）
- Data Quality Tests（資料品質驗證）

<img width="1227" height="757" alt="image" src="https://github.com/user-attachments/assets/99ea187c-b46d-4a63-b108-db73e9419a02" />

---

## Pipeline 狀態監控與追蹤

新增 ETL Tracking 機制，用於記錄 Pipeline 執行狀態與處理結果。

記錄內容包含：

| 欄位                  | 說明                         |
| ------------------- | -------------------------- |
| pipeline_name       | Pipeline 名稱                |
| status              | RUNNING / SUCCESS / FAILED |
| row_count | 本次處理筆數                     |
| last_processed_time | Watermark（來源資料最新時間）        |
| last_run_time       | 最近一次成功執行時間                 |
| created_at          | Pipeline 建立時間              |
| updated_at          | Pipeline 狀態最後更新時間          |


|pipeline_name|last_processed_at|last_run_at|status|row_count|created_at|updated_at|
|-------------|-----------------|-----------|------|---------|----------|----------|
|weather_data_pipeline|2026-06-03 17:16:33.000 +0800|2026-06-03 17:16:35.016 +0800|SUCCESS|218|2026-06-03 16:51:49.966 +0800|2026-06-03 17:16:44.134 +0800|

---

## 失敗通知機制

當 Airflow Task 執行失敗時會自動寄送 Email。

設定：

```python
@task(on_failure_callback=[failure_email])
```

通知內容包含：

- DAG Name
- Task Name
- Execution Date

<img width="1563" height="428" alt="image" src="https://github.com/user-attachments/assets/752f1a78-1c37-42d0-acf6-ab2e74a73005" />

---

## 專案結構

```text
├── dags
│   ├── weather_data_pipeline.py
│   └── include
│       ├── api
│       ├── storage
│       └── transform
├── dbt
│   └── weather_data_platform
│       ├── dbt_project.yml
│       ├── macros
│       └── models
│           ├── staging
│           │   └── source.yml
│           ├── intermediate
│           └── marts
└── docker-compose.yml
```

---

## 執行方式

### 啟動 Airflow

```bash
docker compose up -d
```

### 啟動 MinIO

```bash
docker start minio
```

### 觸發 Pipeline

開啟 Airflow Web UI：

```text
http://localhost:8080
```

手動執行 DAG：

```text
weather_data_pipeline
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

---

## 學習成果

本專案實作並熟悉以下技術：

- Apache Airflow
- TaskFlow API
- MinIO
- PostgreSQL
- dbt
- ELT Pipeline

---

## 未來優化方向

- Add dbt Tests and data validation.
- Implement GitHub Actions CI/CD.
- Refactor Airflow shared modules (`include`).
  
