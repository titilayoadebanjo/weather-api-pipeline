# Weather API ETL Pipeline Using Apache Airflow and PostgreSQL

## Project Overview

This project demonstrates the development of an end-to-end Data Engineering pipeline using Apache Airflow, Python, Pandas and PostgreSQL.

The pipeline automatically retrieves live weather data from the Open-Meteo API, processes the JSON response using Python and stores the information in a PostgreSQL database for future reporting and analysis.

This project simulates a real-world data ingestion workflow where data is collected from an external API on a scheduled basis and loaded into a database for historical analysis.


## Business Problem

Many organisations rely on external APIs to provide operational and analytical data.

Manually retrieving weather information is:

- Time-consuming
- Difficult to scale
- Error-prone
- Unsuitable for historical reporting

This project automates the collection and storage of weather data, ensuring information is captured consistently and made available for analysis.



## Solution Architecture

```text
Open-Meteo Weather API
            │
            ▼
Apache Airflow DAG
            │
            ▼
Python Requests
            │
            ▼
Pandas DataFrame
            │
            ▼
PostgreSQL Database
            │
            ▼
Analytics & Reporting
```


## Technology Stack

- Apache Airflow
- Docker
- Python
- Requests
- Pandas
- PostgreSQL
- SQLAlchemy
- WSL2
- Visual Studio Code

---

## Project Workflow

The pipeline performs the following steps:

1. Connects to the Open-Meteo API.
2. Retrieves current weather data.
3. Extracts relevant data fields from the JSON response.
4. Converts the data into a Pandas DataFrame.
5. Connects to PostgreSQL.
6. Loads
