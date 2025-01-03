from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from kubernetes.client import models as k8s

# DAG 기본 설정
default_args = {
    'owner': 'ehdgml7755@cu.ac.kr',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 24,
    'retry_delay': timedelta(minutes=30),
}

# today_date
today_date = datetime.now().strftime("%Y-%m-%d")

# categoryCode
categoryCode = ["001000", "002000", "103000"]

s3path = f"{today_date}/Musinsa/RankingData/{categoryCode}.parquet"

with DAG(
    dag_id='Musinsa_Ranking_Table_S3_Load_Redshift',
    default_args=default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['musinsa', 'ranking_rawdata', 'Extract', 'Load', 'S3', 'k8s']
) as dag:

    start = DummyOperator(
                task_id="start"
            )

    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        schema='SilverLayer',  # Redshift 스키마
        table='Musinsa_Ranking_silver',    # Redshift 테이블
        s3_bucket='project4-silver-data',
        s3_key=s3path,  # S3 경로
        copy_options=['FORMAT AS PARQUET'],  # Parquet 파일 지정
        aws_conn_id='aws_conn',  # Airflow의 AWS 연결 ID
        redshift_conn_id='redshift_default',  # Airflow의 Redshift 연결 ID
        task_concurrency=1,
        dag=dag,
    )

    s3_to_redshift