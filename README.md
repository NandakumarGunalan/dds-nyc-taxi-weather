# dds-nyc-taxi-weather
Distributed Data Systems group project: NYC Yellow Taxi (2022) + NYC Weather data pipeline (MongoDB, Airflow, Spark SQL, BigQuery)

# Distributed Data Systems Project  
## NYC Yellow Taxi & Weather Data Pipeline (2022)

### Project Overview
This project analyzes the relationship between NYC Yellow Taxi trips and weather conditions for the year **2022**.

We are building a distributed data pipeline using:

- MongoDB (Data Storage)
- Airflow (Orchestration)
- Spark SQL (Distributed Processing)
- BigQuery (Cloud Analytics)

---

## Datasets Used

### 1️⃣ NYC Yellow Taxi Trip Data (2022)
Official NYC Open Data source:

https://data.cityofnewyork.us/Transportation/2022-Yellow-Taxi-Trip-Data/qp3b-zxtp/about_data

- Contains detailed trip-level records
- Includes pickup/dropoff timestamps, fare details, payment types, trip distance, etc.
- We will use only **2022 data**

---

### 2️⃣ NYC Weather Data
Kaggle dataset:

https://www.kaggle.com/datasets/danbraswell/new-york-city-weather-18692022

- Contains daily weather observations from 1869–2022
- For this project, we will filter and use **only 2022 weather data**

---

## Project Goal

To build a distributed data system that:

- Ingests taxi and weather datasets
- Stores data in MongoDB
- Uses Spark SQL for distributed analytics
- Orchestrates pipelines using Airflow
- Performs cloud analytics using BigQuery

---

## 📁 Project Structure


