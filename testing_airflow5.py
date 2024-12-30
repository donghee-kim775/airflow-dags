from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

Sexual_Dynamic_Params = [{
    "SEXUAL" : "F"
}, {
    "SEXUAL" : "M"
}, {
    "SEXUAL" : "A"
}
]

AGE_BAND_Dynamic_Params = [{
    "AGE_BAND" : "AGE_BAND_MINOR" # 19세 이하
}, {
    "AGE_BAND" : "AGE_BAND_20" # 20~24세
}, {
    "AGE_BAND" : "AGE_BAND_25" # 25~29세
}, {
    "AGE_BAND" : "AGE_BAND_30"
}, {
    "AGE_BAND" : "AGE_BAND_35"
}, {
    "AGE_BAND" : "AGE_BAND_40"
}, {
    "AGE_BAND" : "AGE_BAND_ALL"
}
]

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

    # Start task
    start = DummyOperator(
        task_id="start")

    # Number of dynamic tasks to create
    num_tasks = 3

    for i, sexual_dct in enumerate(Sexual_Dynamic_Params):
        # Sexual Dummy Task
        sexual_task = DummyOperator(
            task_id=f"sexual_task_{sexual_dct['SEXUAL']}"
        )
        start >> sexual_task
        for j, age_dct in enumerate(AGE_BAND_Dynamic_Params):
            task = KubernetesPodOperator(
                task_id=f'fetch_data_task_{sexual_dct["SEXUAL"]}_{age_dct["AGE_BAND"]}',
                name=f'fetch_data_task_{sexual_dct["SEXUAL"]}_{age_dct["AGE_BAND"]}',
                namespace='airflow',
                image='ehdgml7755/project4-custom:python-custom',
                cmds=['python', './pythonscripts/Musinsa_Ranking_RawData_EL.py'],  # Execute the script from /scripts
                arguments=[sexual_dct["SEXUAL"], age_dct["AGE_BAND"]],  # Pass task_number as an argument to the script
                is_delete_operator_pod=True,  # Do not delete pod after completion
                get_logs=True,
            )
            sexual_task >> task
            