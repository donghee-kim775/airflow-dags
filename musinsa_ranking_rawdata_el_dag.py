from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
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

SEXUAL_CATEGORY_DYNAMIC_PARAMS = [
    {"SEXUAL": "M", "CATEGORIES": {
        "Shoes": ["103004", "103005", "103001", "103003", "103002", "103007"],
        "Pants": ["003000", "003002", "003004", "003005", "003006", "003008", "003009"],
        "Knitwear": ["001006", "002020"],
        "Shirts": ["001002"],
        "TShirts": ["001001", "001010", "001011", "001005", "001004", "001003"],
        "Jackets": ["002001", "002004", "002002", "002017", "002006", "002022", "002019", "002018"],
        "Coats": ["002008"],
    }},
    {"SEXUAL": "F", "CATEGORIES": {
        "Shoes": ["103001", "103002", "103003", "103004", "103005", "103007"],
        "Skirts": ["100004", "100005", "100006"],
        "Dresses": ["100001", "100002", "100003"],
        "Pants": ["003002", "003004", "003005", "003006", "003007", "003008", "003009"],
        "Knitwear": ["001006", "001008", "002020", "002016"],
        "Shirts": ["001002"],
        "TShirts": ["001001", "001003", "001004", "001005", "001010", "001011"],
    }}
]

# DAG 정의
with DAG(
    dag_id='Musinsa_Ranking_RawData_EL_DAG',
    default_args=default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['MUSINSA', 'RANKING_RAWDATA', 'EXTRACT', 'LOAD', 'S3', 'K8S']
) as dag:

    # Start task
    start = DummyOperator(
                task_id="start"
    )

    # end task
    end = DummyOperator(
                task_id="end"
    )
    
    for sexual_dct in SEXUAL_CATEGORY_DYNAMIC_PARAMS:
        sexual_task = DummyOperator(
                            task_id = f'{sexual_dct["SEXUAL"]}_task'
        )
        start >> sexual_task
        
        # await task
        wait = DummyOperator(
                task_id=f'{sexual_dct["SEXUAL"]}_wait_task'
        )
        
        for category, items in sexual_dct["CATEGORIES"].items():
            category_task = KubernetesPodOperator(
                                task_id=f'{sexual_dct["SEXUAL"]}_{category}_task',
                                name=f'{sexual_dct["SEXUAL"]}_{category}_task',
                                namespace='airflow',
                                image='ehdgml7755/project4-custom:latest',
                                cmds=['python', './pythonscript/musinsa_ranking_rawdata_el.py'],
                                arguments=[sexual_dct["SEXUAL"], category] + items,
                                is_delete_operator_pod=True,
                                get_logs=True,
            )
            sexual_task >> category_task >> wait
        
        wait >> end