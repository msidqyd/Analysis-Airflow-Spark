import os
import pyspark
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col
from dotenv import load_dotenv
from pathlib import Path



# Load Env from .env file.
load_dotenv(dotenv_path=Path('/resources/.env'))

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Create SparkSession with master.
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName('Retail')
    
))
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Configure JBDC.
jdbc_url = f"jdbc:postgresql://{postgres_host}/{postgres_dw_db}"
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read from Postgres.
retail_df = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

# Create Schema.
conn = psycopg2.connect(
    dbname=postgres_dw_db,
    user=postgres_user,
    password=postgres_password,
    host=postgres_host
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS analysis;")
cur.execute("CREATE SCHEMA IF NOT EXISTS cleaned_retail_data;")


# Clean retail_df, keep the null on unit price with assumption promotion or refund, also keep the null description.
retail_df = retail_df.dropDuplicates()
for c in retail_df.columns:
    if dict(retail_df.dtypes)[c] == 'string':
        retail_df = retail_df.withColumn(c, trim(col(c)))
retail_df.createOrReplaceTempView("retail_cleaned")

# Churn Active Query.
churn_Active = spark.sql("""
 WITH cust_loyalty AS (
    SELECT 
        customerid, 
        date_format(invoicedate, 'yyyy-MM') AS month_year,
        SUM(unitprice * quantity) AS total_sales
    FROM retail_cleaned
    WHERE customerid IS NOT NULL
    GROUP BY customerid, date_format(invoicedate, 'yyyy-MM')
),
cust_month_count AS (
    SELECT 
        customerid,
        COUNT(DISTINCT month_year) AS active_months
    FROM cust_loyalty
    WHERE total_sales > 0
    GROUP BY customerid
),
total_months AS (
    SELECT 
        COUNT(DISTINCT date_format(invoicedate, 'yyyy-MM')) AS total_months
    FROM retail_cleaned
)
SELECT 
    COUNT(c.customerid) AS loyal_customers_all_months
FROM 
    cust_month_count c,
    total_months t
WHERE 
    c.active_months = t.total_months
""")
churn_Active.show(10)

# Total_sales_each_month.
sales_each_month = spark.sql("""
    SELECT 
    date_format(invoicedate, 'yyyy-MM') AS month_year, 
    CAST(SUM(unitprice * quantity) AS DECIMAL(10,3)) AS total_sales
    FROM retail_cleaned
    WHERE customerid IS NOT NULL OR quantity > 0
    GROUP BY month_year
    ORDER BY month_year
""")
sales_each_month.show(10)

# customer connected to transaction each months.
customer_attached = spark.sql("""
    SELECT 
    date_format(invoicedate, 'yyyy-MM') AS month_year, 
    COUNT(DISTINCT customerid) as total_customer
    FROM retail_cleaned
    WHERE customerid IS NOT NULL
    GROUP BY month_year
    ORDER BY month_year
""")
customer_attached.show(10)

# The most ordered stock.
stock_code_sales = spark.sql("""
    SELECT 
    stockcode, 
    SUM(quantity) as total_sales
    FROM retail_cleaned
    WHERE quantity > 0
    GROUP BY stockcode
    ORDER BY total_sales DESC
""")
stock_code_sales.show(10)

# The most refund stock.
stock_code_minus = spark.sql("""
    SELECT 
    stockcode, 
    SUM(quantity) as total_refund
    FROM retail_cleaned
    WHERE quantity < 0
    GROUP BY stockcode
    ORDER BY total_refund ASC
""")
stock_code_minus.show(10)

#Save All to clean_data and analysis Schema in Postgres
retail_df.write.jdbc(jdbc_url, "cleaned_retail_data.retail", mode="overwrite", properties=jdbc_properties)
churn_Active.write.jdbc(jdbc_url, "analysis.churn_active", mode="overwrite", properties=jdbc_properties)
sales_each_month.write.jdbc(jdbc_url, "analysis.sales_each_month", mode="overwrite", properties=jdbc_properties)
customer_attached.write.jdbc(jdbc_url, "analysis.customer_attached", mode="overwrite", properties=jdbc_properties)
stock_code_sales.write.jdbc(jdbc_url, "analysis.stock_code_sales", mode="overwrite", properties=jdbc_properties)
stock_code_minus.write.jdbc(jdbc_url, "analysis.stock_code_minus", mode="overwrite", properties=jdbc_properties)

#Check the table already loaded.
schemas = ['analysis', 'cleaned_retail_data']
for schema in schemas:
    print(f"\nTables in schema '{schema}':")
    cur.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s
    """, (schema,))
    tables = cur.fetchall()
    for table in tables:
        print(f"- {table[0]}")

cur.close()
conn.close()
spark.stop()
