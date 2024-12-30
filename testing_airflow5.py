from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 실행할 Python 함수 정의
def fetch_data(task_number):
    import requests
    url = f"https://jsonplaceholder.typicode.com/posts/{task_number}"
    response = requests.get(url)
    print(f"Task {task_number} fetched data: {response.json()}")

# DAG 정의
with DAG(
    dag_id='k8s_executor_with_dynamic_python_tasks_2',
    default_args=default_args,
    description='Example DAG using dynamic Python tasks in KubernetesExecutor',
    schedule_interval=None,  # 수동 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'k8s', 'dynamic-python'],
) as dag:

        # Start
    start = DummyOperator(
        task_id="start")

    # 동적으로 생성할 태스크 수
    num_tasks = 3

    # 동적 태스크 생성
    for i in range(1, num_tasks + 1):
        task = KubernetesPodOperator(
            task_id=f'fetch_data_task_{i}',
            name=f'fetch_data_task_{i}',
            namespace='airflow',
            image='python:3.9-slim',
            cmds=['python', '-c'],
            arguments=[f"from __main__ import fetch_data; fetch_data({i})"],
            is_delete_operator_pod=False,  # 작업 완료 후 파드 삭제 안 함
            get_logs=True,
        )
        start >> task
