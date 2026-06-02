# Weather Data Platform

使用 Airflow、MinIO、PostgreSQL 與 dbt 建置的端到端天氣資料平台。

---

## 簡介

本專案旨在練習並實作資料工程常見技術，包括 Airflow 工作流編排、MinIO Data Lake、PostgreSQL Data Warehouse、dbt 資料建模以及 ELT Pipeline 設計。

資料處理流程如下：

- 將 API 原始資料以 JSON 格式存放於 MinIO Data Lake Bronze Layer
- 進行資料清洗、欄位轉換與結構化處理後，存放於 MinIO Data Lake Silver Layer
- 將整理後的資料載入 PostgreSQL 作為 Data Warehouse
- 使用 dbt 建立商業邏輯與資料模型（Data Modeling）
- 產生供分析使用的 Data Mart，形成完整的 ELT（Extract, Load, Transform）Pipeline
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
```

取得資料內容：

- 地區名稱
- 天氣現象
- 最高溫
- 最低溫
- 降雨機率
- 體感溫度

---

#### Bronze Layer

原始 API 回傳資料存入 MinIO。

格式：

```json
weather_forecast.json
```

目的：

- 保留原始資料
- 支援資料追溯
- 支援重新處理

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
- 日期格式轉換
- Null Value 處理

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
bronze = scrape_weatherforecast()

silver = trans_minio_silver(bronze)

staging = ins_postgres_staging(silver)

bronzeAlert = scrape_hightempalert()

silverAlert = trans_minio_silver_alert(bronzeAlert)

stagingAlert = ins_postgres_staging_alert(silverAlert)

dbt_job = run_dbt()
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

### 模組化設計

依功能拆分：

```text
api/
storage/
transform/
```

降低程式耦合度，提高維護性。

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
  
