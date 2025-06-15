from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
from airflow.utils.dates import days_ago



default_args = {
    "owner" : "retail_data",
    "retry_delay" : timedelta(minutes=5),
}

dag = DAG(
    dag_id = "etl_spark_airflow_retail_dag",
    default_args = default_args,
    schedule_interval = None,
    dagrun_timeout = timedelta(minutes=120),
    description = "Airflow with Spark processing for retail table.",
    start_date = days_ago(1),
)

file_path = "/spark-scripts"

elt = SparkSubmitOperator(
    application = f"{file_path}/spark-retail-etl.py",
    conn_id = "spark_main",
    task_id = "spark_elt",
    packages="org.postgresql:postgresql:42.2.18",
    dag = dag,
)

elt
