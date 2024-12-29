from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 12, 1),
    "catchup": False,
}

with DAG(
    dag_id="kubernetes_pod_operator_example",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    task = KubernetesPodOperator(
        namespace="airflow",  # Pod가 생성될 네임스페이스
        image="python:3.9",  # 실행할 컨테이너 이미지
        cmds=["python", "-c"],
        arguments=["print('Hello from the KubernetesPodOperator!')"],
        name="example-pod-task",
        task_id="run-pod-task",
        is_delete_operator_pod=True,  # 작업 완료 후 Pod 삭제
        get_logs=True,  # 로그 가져오기 활성화
    )

    task
