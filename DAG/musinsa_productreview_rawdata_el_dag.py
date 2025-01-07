from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

import json
from datetime import datetime

from DAG.modules.musinsa_mappingtable import CATEGORY2DEPTH_MAPPING, mapping2depth_en, mapping3depth_en
from DAG.modules.config import DEFAULT_DAG

# DAG 정의
with DAG(
    dag_id='Musinsa_ProductReview_RawData_EL_DAG',
    default_args=DEFAULT_DAG.default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=datetime(2025, 1, 1, tzinfo=DEFAULT_DAG.local_tz),
    catchup=False,
    tags=['MUSINSA', 'REVIEW_RAWDATA', 'EXTRACT', 'LOAD', 'S3', 'K8S']
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
            task_id=f"{mapping2depth_en(category2depth)}_task"
        )
        
        wait_task = DummyOperator(
            task_id=f"{mapping2depth_en(category2depth)}_wait"
        )
        
        start >> category2depth_task
        
        for category3depth in category3depth_list:
            category3depth_task = KubernetesPodOperator(
                task_id=f"review_{mapping3depth_en(category3depth[0])}_task",
                name=f"review_{mapping3depth_en(category3depth[0])}_task",
                namespace='airflow',
                image='ehdgml7755/project4-custom:latest',
                cmds=["python", "./pythonscript/musinsa_product_review_rawdata_el.py"],
                arguments = [category3depth[0], json.dumps(category3depth[1])],
                is_delete_operator_pod = True,
                get_logs = True
            )
            
            category2depth_task >> category3depth_task >> wait_task
        
        wait_task >> end