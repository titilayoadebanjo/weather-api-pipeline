# Weather Data Engineering Pipeline with Data Quality Validation and Power BI Reporting
![Python](https://img.shields.io/badge/Python-3.12-blue
![Apache Airflow] (https://img.shields.io/badge/Apache%20Airflow-Orchestration-red
![PostgreSQL] (https://img.shields.io/badge/PostgreSQL-Database-blue
![Power BI] (https://img.shields.io/badge/PowerBI-Dashboard-yellow
![Docker] (https://img.shields.io/badge/Docker-Containerisation-blue

## Project Overview

This project demonstrates the development of an end-to-end Data Engineering pipeline using Apache Airflow, Python, PostgreSQL and Power BI.

The pipeline retrieves live weather data from the Open-Meteo API, validates the data using quality rules, stores it in PostgreSQL, automatically exports the processed data to a CSV file and visualises the results in a Power BI dashboard.

The solution demonstrates a complete modern data workflow covering ingestion, validation, storage, automation and reporting.

---

## Business Problem

Many organisations rely on external APIs for operational and analytical reporting.

However, API data can sometimes contain:

- Missing values
- Invalid measurements
- Unexpected data formats
- Failed responses

Without data quality checks, inaccurate data can be loaded into reporting systems and affect business decisions.

This project addresses these challenges by implementing automated validation before loading weather data into PostgreSQL.

---

## Solution Architecture

```text
Open-Meteo API
       │
       ▼
Data Quality Validation
       │
       ▼
Apache Airflow
       │
       ▼
PostgreSQL
       │
       ▼
Automated CSV Export
       │
       ▼
Power BI Dashboard
```

---

## Technology Stack

### Data Engineering

- Apache Airflow
- Python
- PostgreSQL
- SQLAlchemy
- Docker

### Data Processing

- Pandas
- Requests

### Business Intelligence

- Power BI

### Development Environment

- Visual Studio Code
- WSL2
- Docker Desktop

---

## Project Workflow

### Step 1: Extract Weather Data

The pipeline retrieves live weather observations from the Open-Meteo API.

**Weather attributes collected:**

- Observation Time
- Temperature
- Wind Speed

---

### Step 2: Validate Data Quality

The pipeline validates data before loading.

**Validation checks include:**

#### API Validation

```python
if response.status_code != 200:
    raise ValueError("API request failed")
```

#### Missing Temperature Check

```python
if weather["temperature_2m"] is None:
    raise ValueError("Temperature is missing")
```

#### Missing Wind Speed Check

```python
if weather["wind_speed_10m"] is None:
    raise ValueError("Wind speed is missing")
```

#### Temperature Range Validation

```python
if weather["temperature_2m"] < -50:
    raise ValueError("Temperature is unrealistically low")
```

```python
if weather["temperature_2m"] > 60:
    raise ValueError("Temperature is unrealistically high")
```

#### Wind Speed Validation

```python
if weather["wind_speed_10m"] < 0:
    raise ValueError("Wind speed cannot be negative")
```

If validation fails:

```text
Task Fails
     ↓
Pipeline Stops
     ↓
No Data Loaded
```

---

### Step 3: Load Data into PostgreSQL

Validated records are loaded into PostgreSQL.

```python
df.to_sql(
    "weather_data",
    engine,
    if_exists="append",
    index=False
)
```

---

### Step 4: Automated CSV Export

Following successful database loading, the pipeline automatically exports all weather records to a CSV file.

```python
df.to_csv(
    "/opt/airflow/data/weather_data.csv",
    index=False
)
```

This file serves as the reporting dataset for Power BI.

---

### Step 5: Power BI Reporting

Power BI consumes the automatically generated weather dataset.

The dashboard provides:

- Temperature trend analysis
- Wind speed trend analysis
- KPI reporting
- Observation history
- Interactive date filtering

---

## Airflow Workflow

### DAG Name

```text
weather_pipeline
```

### Schedule

```text
Hourly
```

### Task Flow

```text
validate_weather
         ↓
load_weather
         ↓
export_weather_csv
```

---

## Database Design

### Table: weather_data

```sql
CREATE TABLE weather_data (
    observation_time TIMESTAMP,
    temperature NUMERIC,
    windspeed NUMERIC
);
```

---

## Power BI Dashboard

### Dashboard Title

```text
Manchester Weather Monitoring Dashboard
```

### Key Performance Indicators (KPIs)

- Average Temperature
- Maximum Temperature
- Maximum Wind Speed
- Observations Collected

### Visualisations

#### Temperature Trend Over Time

Tracks changes in temperature over time.

#### Wind Speed Trend Over Time

Tracks changes in wind speed over time.

#### Temperature Distribution

Displays weather variation across observations.

#### Latest Weather Observations Table

Shows the most recent weather data captured by the pipeline.

#### Interactive Date Filter

Allows users to analyse selected time periods.

---

## Dashboard Architecture

```text
Open-Meteo API
       ↓
Apache Airflow
       ↓
PostgreSQL
       ↓
Automated CSV Export
       ↓
Power BI Dashboard
```

---

## Results

The solution successfully:

- Retrieves weather data from a live API
- Applies automated quality validation
- Loads clean data into PostgreSQL
- Exports reporting datasets automatically
- Updates reporting data after every pipeline execution
- Provides visual analytics through Power BI

### Example Output

```text
Observation Time       Temperature    Wind Speed
------------------------------------------------
07/09/2026 10:00       18.4           12.2
07/09/2026 11:00       18.8           11.9
07/09/2026 12:00       19.1           11.3
07/09/2026 13:00       19.4           10.8
```

---

## Skills Demonstrated

### Data Engineering

- ETL Development
- Workflow Orchestration
- API Integration
- Data Validation
- Data Quality Monitoring
- Data Ingestion Automation
- Automated Reporting

### Python

- Requests
- Pandas
- SQLAlchemy

### Database Technologies

- PostgreSQL
- SQL Development
- Database Connectivity

### Reporting and Analytics

- Power BI
- DAX Measures
- KPI Dashboards
- Data Visualisation

### Infrastructure

- Docker
- Apache Airflow
- WSL2

---

## Repository Structure

```text
weather-api-pipeline/
│
├── dags/
│   └── weather_pipeline.py
│
├── data/
│   └── weather_data.csv
│
├── screenshots/
│   ├── airflow_dag.png
│   ├── successful_run.png
│   ├── postgres_results.png
│   └── dashboard.png
│
├── README.md
│
└── .gitignore
```

---

## Screenshots

### Airflow DAG

Shows workflow orchestration and task dependencies.

### Successful Pipeline Execution

Shows successful execution of validation, loading and export tasks.

### PostgreSQL Results

Shows weather observations stored in PostgreSQL.

### Power BI Dashboard

Displays weather trends, KPIs and reporting insights.

---

## Future Enhancements

- Direct PostgreSQL-to-Power BI connectivity
- Power BI Service scheduled refresh
- Email alerts on pipeline failures
- Multi-location weather monitoring
- Azure Data Factory integration
- Azure SQL Database deployment
- Data Lake integration
- dbt transformations
- Historical weather forecasting analytics

---

## Lessons Learned

This project provided practical experience in:

- Building end-to-end data pipelines
- Consuming REST APIs
- Processing JSON data
- Implementing data quality controls
- Developing Apache Airflow DAGs
- Working with PostgreSQL databases
- Automating reporting workflows
- Designing Power BI dashboards
- Deploying Data Engineering solutions using Docker

---

## Author

**Titilayo Adebanjo**

Senior Data Scientist | Aspiring Data Engineer

This project demonstrates the implementation of a production-style weather analytics platform incorporating data ingestion, quality assurance, workflow orchestration, database storage, automated reporting and business intelligence visualisation.
