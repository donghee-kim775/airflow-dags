from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago

from datetime import datetime
import pendulum

from DAG.modules.musinsa_mappingtable import SEXUAL_CATEGORY_DYNAMIC_PARAMS
from DAG.modules.config import DEFAULT_DAG

import json

local_tz = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
    dag_id='Musinsa_Ranking_RawData_EL_DAG',
    default_args=DEFAULT_DAG.default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
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
    
    # SEXUAL_CATEGORY_DYNAMIC_PARAMS
    # dct_1 => 여성 dct / dct_2 => 남성 dct
    for dct in SEXUAL_CATEGORY_DYNAMIC_PARAMS:
        sexual = list(dct['SEXUAL'].items())[0]
        
        sexual_task = DummyOperator(
            task_id=f"{sexual[0]}_task"
        )
        
        wait_task = DummyOperator(
            task_id=f"{sexual[0]}_wait"
        )
        
        start >> sexual_task
        for categories in dct['CATEGORIES']:
            category2depth = list(categories.items())[0]
            
            category_task = KubernetesPodOperator(
                task_id=f"{sexual[0]}_{category2depth[0]}_task",
                name=f'{sexual[0]}_{category2depth[0]}_task',
                namespace='airflow',
                image='ehdgml7755/project4-custom:latest',
                cmds=['python', './pythonscript/musinsa_ranking_rawdata_el.py'],
                arguments=[json.dumps(sexual), json.dumps(category2depth)],
                is_delete_operator_pod=False,
                get_logs=True,
            )
            
            sexual_task >> category_task >> wait_task
        
        wait_task >> end
