import logging
import requests
import os
import json

from datetime import datetime, timedelta

import time, random

import shutil

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.decorators import task

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

Sexual_Dynamic_Task = [{
    "SEXUAL" : "F"
}, {
    "SEXUAL" : "M"
}, {
    "SEXUAL" : "A"
}
]

AGE_BAND_Dynamic_Task = [{
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

def start_dag():
    logging.info("DAG 시작")

def end_dag():
    logging.info("DAG 종료")

##########################
######### sexual #########
##########################
@task
def sexual_function_with_input(value: str):
    start_time = datetime.now()
    print(f"Task 시작 시간: {start_time}")
    
    print(value)
    time.sleep(10)
    end_time = datetime.now()
    print(f"Task 종료 시간: {end_time}")

###########################
######### ageband #########
###########################
@task
def ageband_function_with_input(value: str):
    print(value)

###########################
######### request #########
###########################
@task
def Musinsa_Ranking_Request(request_params: dict):
    start_time = datetime.now()
    print(f"Crawl_Task 시작 시간: {start_time}")
    url = "https://api.musinsa.com/api2/hm/v2/pans/ranking/sections/199?"
    
    categoryCode = ["103000", "002000", "001000"]
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    for code in categoryCode:
        # 랜덤한 대기 시간 생성 (2초에서 5초 사이)
        sleep_time = random.uniform(2, 5)
        time.sleep(sleep_time)
        
        # 디렉토리 생성
        dir_path = os.path.join("/opt/airflow/dags/test_json", "Raw_Data", today_date, code)
        
        # 디렉토리 유효성 검증
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)
        
        # 파일 이름 생성
        file_name = f"musinsa_{request_params['gf']}_{request_params['ageBand']}_{code}.json"
        file_path = os.path.join(dir_path, file_name)
        
        request_params['categoryCode'] = code
        
        print("***** request_params *****")
        print(request_params)
        
        response = requests.get(url, headers=headers, params=request_params)
        
        # 응답을 JSON 형식으로 로컬 파일에 저장
        if response.status_code == 200:
            response_data = response.json()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, ensure_ascii=False, indent=4)
                
            print(f"Response saved to {file_path}")
        else:
            print(f"Failed to fetch data for {request_params}. Status code: {response.status_code}")
 
###########################
######### waiting #########
###########################   
@task
def product_id_task(value: dict):
    print(value, "task complete >> product_id")

###########################
######### waiting #########
###########################
@task
def wait_task(value: str):
    print(value, "task complete >> wait")

default_args = {
    'start_date' : datetime(2024,12,1),
    'retries' : 0,
    'retry_delay' : timedelta(minutes=60),
}

with DAG(
    'Musinsa_Ranking_EL_DAG',
    default_args=default_args,
    description='무신사 랭킹 상품 데이터 수집',
    schedule_interval='0 0 * * *',  # 매일 자정에 실행
    catchup=False,  # 과거 실행 날짜에 대해 실행하지 않음
    max_active_runs=5,  # 동시에 실행될 수 있는 DAG 실행의 최대 수
) as dag:
    
    start = PythonOperator(
        task_id = 'start',
        python_callable = start_dag,
    )
    
    end = PythonOperator(
        task_id = 'end',
        python_callable = end_dag,
    )
    
    for i, sexual_dct in enumerate(Sexual_Dynamic_Task):
        sexual_execute_together = sexual_function_with_input.override(task_id=f"{sexual_dct['SEXUAL']}_task")(sexual_dct["SEXUAL"])
        execute_wait = wait_task.override(task_id=f"{sexual_dct['SEXUAL']}_wait_task")(sexual_dct["SEXUAL"])
        
        start >> sexual_execute_together
        
        for j, age_dct in enumerate(AGE_BAND_Dynamic_Task):
            age_execute_together = ageband_function_with_input.override(task_id=f"{sexual_dct['SEXUAL']}_{age_dct['AGE_BAND']}_task")(age_dct["AGE_BAND"])
            
            params = {
                      "storeCode" : "musinsa",
                      "gf": sexual_dct["SEXUAL"],
                      "ageBand": age_dct["AGE_BAND"],
                      "period" : "DAILY"
                      }
            musinsa_execute_together = Musinsa_Ranking_Request.override(task_id=f"{sexual_dct['SEXUAL']}_{age_dct['AGE_BAND']}_request_task")(params)
            sexual_execute_together >> age_execute_together >> musinsa_execute_together >> execute_wait >> end