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

CATEGORY_PARAMS = {
    "Top" : {
        "Shirts": ["001002"],
        "Knitwear": ["001006", "001008", "002016", "002020"],
        "TShirts": ["001001", "001003", "001004", "001005", "001010", "001011"],
        "Dresses": ["100001", "100002", "100003"]
    },
    "Outer" : {
        "Jackets": ["002001", "002002", "002004", "002006", "002017", "002018", "002019", "002022"],
        "Coats": ["002008"],
    },
    "Bottom" : {
        "Pants": ["003000", "003002", "003004", "003005", "003006", "003007", "003008", "003009"],
        "Skirts": ["100004", "100005", "100006"],
        "Shoes": ["103001", "103002", "103003", "103004", "103005", "103007"]
    }
}

# today_date
today_date = datetime.now().strftime("%Y-%m-%d")

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

    end = DummyOperator(
        task_id="end"
    )
    
    for categorydepth in CATEGORY_PARAMS.keys():
        categorydepth_task = DummyOperator(
            task_id=f"{categorydepth}_task"
        )
        start >> categorydepth_task

        for category, categorycodes in CATEGORY_PARAMS[categorydepth].items():
            category_task = DummyOperator(
                task_id=f"{category}_task"
            )
            categorydepth_task >> category_task

            # 순차 연결을 위한 초기값 설정
            previous_task = category_task

            wait = DummyOperator(
                task_id=f"{categorydepth}_wait_task"
            )
            
            for categorycode in categorycodes:
                categorycode_task = DummyOperator(
                    task_id=f"{categorycode}_task"
                )
                
                previous_task >> categorycode_task

                previous_task = categorycode_task

            previous_task >> wait
        
        wait >> end
            