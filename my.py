from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Nikita',
    'depends_on_past': False,
    'email': ['Nikita_Zaletov@epam.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'spark_load',
    default_args=default_args,
    description='Parquet -> Avro DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    t1 = SparkSubmitOperator(
        task_id='run_spark',
        application='/opt/airflow/airflow-test_2.12-0.1.jar',
        java_class='Load',
        packages='org.apache.spark:spark-avro_2.12:3.1.1',
        application_args=['/opt/airflow/infile.parquet', '/opt/airflow/out-dir-avro'],
        spark_binary='spark-submit',
        conn_id='spark_local'
    )

    dag.doc_md = __doc__
    dag.doc_md = """
    Spark job is converting data from Parquet to Avro
    """