![Screenshot 2025-06-15 224732](https://github.com/user-attachments/assets/45f1cd55-9ee5-4869-b687-f790deb13bf7)
# 🌀 Batch Processing of Retail Data with Airflow & PySpark

This project demonstrates how to perform **batch data processing** using **Apache Airflow** and **PySpark**, with output written to a **PostgreSQL** data warehouse. 

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



---

## 📘 Documentation
![Screenshot 2025-06-15 224745](https://github.com/user-attachments/assets/3d759bcf-e07e-4f45-bcac-880892f0921f)
![Screenshot 2025-06-15 224255](https://github.com/user-attachments/assets/29bfcca6-9177-4288-8f9d-f22fbff2b1c3)
![Screenshot 2025-06-15 224454](https://github.com/user-attachments/assets/55625632-37f3-41f2-92d3-2c3b4aa92af9)

---
