# 🌀 Batch Processing of Retail Data with Airflow & PySpark

This project demonstrates how to perform **batch data processing** using **Apache Airflow** and **PySpark**, with output written to a **PostgreSQL** data warehouse. It automates a full ETL pipeline that loads, cleans, transforms, and analyzes retail data.

---

## ⚙️ Airflow DAG Workflow

- **DAG Name:** `spark_airflow_retail_dag`
- **Task:** Submit a Spark job (`spark-retail-etl.py`) using the `SparkSubmitOperator`
- **Schedule:** `None` (manual trigger)
- **Timeout:** 120 minutes per run
- **Spark Connection ID:** `spark_main` (configured in Airflow connections)
- **Python Operator Used:** `SparkSubmitOperator`

The DAG triggers a Spark job that executes all ETL logic and analysis in one batch run.

---

## 🔄 ETL Process (PySpark)

The PySpark script performs the following ETL steps:

1. **Environment Setup**  
   Loads environment variables from `.env` and initializes a SparkSession.

2. **Data Loading**  
   Reads raw data from the `public.retail` table in PostgreSQL.

3. **Data Cleaning**  
   - Drops duplicates  
   - Trims string columns  
   - Keeps NULLs in `unitprice` and `description` (assumed to be refunds/promos)

4. **Schema Initialization**  
   Creates target schemas if not present:
   - `cleaned_retail_data`
   - `analysis`

5. **Transformation**  
   Data is transformed and stored as temporary views for SQL-based operations.

---

## 📊 Batch Analysis Performed

The following batch queries are executed and results written to PostgreSQL:

| Table Name | Description |
|------------|-------------|
| `cleaned_retail_data.retail` | Cleaned version of the original retail dataset |
| `analysis.churn_active` | Count of customers who made purchases in every month (loyal customers) |
| `analysis.sales_each_month` | Total sales (unitprice * quantity) aggregated per month |
| `analysis.customer_attached` | Number of distinct customers attached to transactions per month |
| `analysis.stock_code_sales` | Most frequently purchased stock items (positive quantity) |
| `analysis.stock_code_minus` | Most refunded/returned stock items (negative quantity) |

---

## 🧪 How to Run
1. **Clone this Repo**
> **Note:** This project environment, docker, and dev.nix setup was previously cloned from  
> [https://github.com/thosangs/dibimbing_spark_airflow](https://github.com/thosangs/dibimbing_spark_airflow)


2. **Start Airflow & Spark Environment**
   - `make docker-build`
   - `make spark`
   - `make postgres`
   - `make airflow`

3. **Trigger DAG**
   - Manually from Airflow UI or API.

---

## 📂 Output

Final analysis results are written into **PostgreSQL** under:

- Schema: `analysis`
- Schema: `cleaned_retail_data`

Accessible via DBeaver, pgAdmin, or through any PostgreSQL client.

---

## 📘 Documentation


---
