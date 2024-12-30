from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='k8s_executor_with_python_base',
    default_args=default_args,
    description='Example DAG using Python base image in KubernetesExecutor',
    schedule_interval=None,  # 수동 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'k8s', 'python-base'],
) as dag:

    # 실행할 Python 함수 정의
    def fetch_data(task_number):
        import requests  # 기본 이미지에는 미리 설치되어 있지 않음
        url = f"https://jsonplaceholder.typicode.com/posts/{task_number}"
        response = requests.get(url)
        time.sleep(10)
        print(f"Task {task_number} fetched data: {response.json()}")

    # 병렬 태스크 생성
    parallel_tasks = []
    for i in range(1, 4):
        task = PythonOperator(
            task_id=f'fetch_data_task_{i}',
            python_callable=fetch_data,
            op_args=[i],
            executor_config={
                "KubernetesExecutor": {
                    "image": "python:3.9-slim",  # Python 기본 이미지
                    "resources": {
                        "requests": {"memory": "128Mi", "cpu": "250m"},
                        "limits": {"memory": "256Mi", "cpu": "500m"},
                    },
                }
            },
        )
        parallel_tasks.append(task)

    # 병렬 태스크는 별도의 의존성 없이 실행
