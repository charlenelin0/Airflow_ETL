# Weather Data Platform

使用 Airflow、MinIO、PostgreSQL 與 dbt 建置的端到端天氣資料平台。

---

## 簡介

本專案旨在實作資料工程常見技術，包括 Airflow 工作流編排、dbt 資料建模以及 ELT Pipeline 設計。

資料處理流程如下：

- 透過 [中央氣象署開放資料平臺 API](https://opendata.cwa.gov.tw/dist/opendata-swagger.html) 定期擷取天氣預報與高溫警報資料
- 將原始資料以 JSON 格式存放於 MinIO Data Lake Bronze Layer，保留完整來源資料供追溯與重跑使用
- 進行資料展平、欄位標準化及結構化轉換，並以 Parquet 格式存放於 MinIO Data Lake Silver Layer
- 將整理後的資料載入 PostgreSQL Data Warehouse 的 Staging Layer
- 使用 dbt 建立 Fact / Dimension Model，實作資料建模（Data Modeling）與商業邏輯轉換
- 建立完整 ELT（Extract, Load, Transform）資料管線，提供天氣資料分析與查詢使用
  
---

## 系統架構

```text
                     Weather API
                          │
                          ▼
                 Apache Airflow
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
      Weather Forecast          High Temp Alert
            │                           │
            ▼                           ▼
        Bronze Layer (Raw JSON in MinIO)
                          │
                          ▼
        Silver Layer (Clean CSV in MinIO)
                          │
                          ▼
          PostgreSQL Staging Tables
                          │
                          ▼
                    dbt Models
                          │
                          ▼
                    Data Mart
```

---

## 技術架構

| 類別 | 技術 |
|--------|--------|
| Workflow Orchestration | Apache Airflow |
| Programming Language | Python |
| Data Lake | MinIO |
| Data Warehouse | PostgreSQL |
| Data Transformation | dbt |
| Containerization | Docker |
| Notification | SMTP Email |

---

## 資料流程

### 1. Weather Forecast ETL

#### Extract

透過 Weather API 擷取天氣預報資料。

Airflow Task：

```python
scrape_weatherforecast()

scrape_hightempalert()
```

取得資料內容：

- 一般天氣預報-今明 36 小時天氣預報
- 天氣特報-高溫資訊

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
trans_minio_silver()
```

處理內容：

- JSON Flatten
- 欄位命名標準化

輸出：

```csv
weather_forecast.csv
```

---

#### Load to PostgreSQL

Airflow Task：

```python
ins_postgres_staging()
```

目標資料表：

```sql
stg_weather_forecast
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
stg_weather_alert
```

---

## Airflow DAG

本專案採用 TaskFlow API 開發。

```python
bronzeForecast = scrape_weatherforecast()

silverForecast = trans_minio_silver(bronzeForecast)

stagingForecast = ins_postgres_staging(silverForecast)

bronzeAlert = scrape_hightempalert()

silverAlert = trans_minio_silver_alert(bronzeAlert)

stagingAlert = ins_postgres_staging_alert(silverAlert)

dbt_job = run_dbt()

[stagingAlert, stagingForecast] >> dbt_job

```

<img width="1440" height="789" alt="weather_data_pipeline-graph" src="https://github.com/user-attachments/assets/873700c3-b844-4d97-b63b-17fb6d672f6d" />

---

## dbt 建模

資料進入 PostgreSQL 後，由 dbt 進行轉換。

執行：

```bash
dbt run
```

產生：

### Staging Layer

```text
stg_weather_forecast
stg_weather_alert
```

### Mart Layer

```text
fact_weather_forecast
fact_weather_alert
```

<img width="1821" height="806" alt="image" src="https://github.com/user-attachments/assets/04bb44ce-f02d-4460-965b-6bbb1f5fbff3" />

提供後續 BI 與分析使用。

---

## 失敗通知機制

為了提高 Pipeline 穩定性，當 Airflow Task 執行失敗時會自動寄送 Email。

設定：

```python
@task(on_failure_callback=[failure_email])
```

通知內容包含：

- DAG Name
- Task Name
- Execution Date
- Error Message

---

## 專案結構

```text
.
├── dags
│   └── daily_quotes_ey.dag.py
│
├── include
│   ├── api
│   │   └── weather_api.py
│   │
│   ├── storage
│   │   └── minio_client.py
│   │
│   ├── transform
│   │   └── weather_transform.py
│   │
│   └── __init__.py
│
├── dbt
│   └── weather_project
│
├── logs
│
├── plugins
│
└── docker-compose.yml
```

---

## 執行方式

### 啟動 Airflow

```bash
docker compose up -d
```

### 手動觸發 DAG

```bash
airflow dags trigger daily_quotes_ey
```

### 執行 dbt

```bash
cd dbt/weather_project

dbt run

dbt test

dbt docs generate

dbt docs serve
```

---

## 專案特色

### Data Lake + Data Warehouse 架構

採用業界常見資料分層：

```text
API
 ↓
Bronze
 ↓
Silver
 ↓
Warehouse
 ↓
Mart
```

---

### 自動化資料處理

透過 Airflow 實現：

- 自動排程
- 自動轉換
- 自動載入
- 自動通知

---

## 學習成果

本專案實作並熟悉以下技術：

- Apache Airflow
- TaskFlow API
- MinIO
- PostgreSQL
- dbt
- Docker
- ELT Pipeline
- Failure Alerting

---

## 未來優化方向

- Incremental Model
- Test Coverage
- GitHub Actions CI/CD
  
