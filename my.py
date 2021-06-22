from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Nikita',
    'depends_on_past': False,
    'email': ['Nikita_Zaletov@epam.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'tutorial',
    default_args=default_args,
    description='Parquet -> Avro DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = SparkSubmitOperator(
        task_id='run_spark',
        application='/opt/airflow/airflow-test_2.12-0.1.jar',
        java_class='Load',
        packages='org.apache.spark:spark-avro_2.12:3.1.1',
        application_args=['/home/nikita/infile.parquet', '/home/nikita/outfile.avro'],
        spark_binary='/opt/airflow/spark-3.1.1-bin-hadoop3.2/bin/spark-submit',
        conn_id='spark_local'
    )

    dag.doc_md = __doc__
    dag.doc_md = """
    Spark job is converting data from Parquet to Avro
    """  # otherwise, type it like this