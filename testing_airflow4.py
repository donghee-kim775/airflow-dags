from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k8s_pod_parallel_execution',
    default_args=default_args,
    description='Execute multiple tasks in parallel within a single Kubernetes Pod',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    parallel_task = KubernetesPodOperator(
        task_id='parallel_task',
        name='parallel-task-pod',
        namespace='default',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=["""
        import concurrent.futures
        def task(n):
            print(f'Task {n} 실행 중')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(task, range(1, 4))
        """],
        resources={
            'request_memory': '128Mi',
            'request_cpu': '250m',
            'limit_memory': '256Mi',
            'limit_cpu': '500m',
        },
    )
