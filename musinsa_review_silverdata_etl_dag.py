from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta

from musinsa_mappingtable import CATEGORY2DEPTH_MAPPING

import json

# DAG 기본 설정
default_args = {
    'owner': 'ehdgml7755@cu.ac.kr',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 24,
    'retry_delay': timedelta(minutes=30),
}

# DAG 정의
with DAG(
    dag_id='Musinsa_Review_SilverData_ETL_DAG',
    default_args=default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['MUSINSA', 'RANKING_RAWDATA', 'EXTRACT', 'LOAD', 'S3', 'K8S']
) as dag:
    
    # start task
    start = DummyOperator(
                task_id="start"
    )

    # end task
    end = DummyOperator(
                task_id="end"
    )
    
    for key in CATEGORY2DEPTH_MAPPING:
        category2depth = key
        category3depth_list = list(CATEGORY2DEPTH_MAPPING[key].items())
    
        category2depth_task = DummyOperator(
            task_id=f"{category2depth}_task"
        )
        
        start >> category2depth_task
        
        for category3depth in category3depth_list:
            category3depth_task = KubernetesPodOperator(
                task_id=f"review_{category3depth[0]}_task",
                name=f"review_{category3depth[0]}_task",
                namespace='airflow',
                image='ehdgml7755/project4-custom:latest',
                cmds=["python", "./pythonscript/musinsa_review_silverdata_etl.py"],
                argument = [category3depth[0], json.dumps(category3depth_list[1])],
                is_delete_operator_pod = False,
                get_logs = True
            )
            
            category2depth_task >> category3depth_task