'''
함수 내부 연산결과 조건부로 task 진행
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
import json
import random
import pandas as pd # 소량의데이터라 spark 대신 pandas
import os

# 2. 환경변수
# 프로젝트 내부 폴더를 데이터용으로 지정
# 향후 s3로 확장 가능

DATA_PATH = '/opt/airflow/dags/data' # 도커 내부에 생성된 컨테이너 상 워커의 데이터폴더
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    # 스마트 팩토리에 설치된 오븐 온도 센서에서 데이터 발생되면 데이터레이크에 쌓인다고 가정
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}",
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H-%M-%S"), # YYYY-MM-DD hh:mm:ss
            "temperature" : round( random.uniform(20.0,150.0),2),
            "status" : "on"
        } for i in range (10)
    ]
    # 더미데이터 파일로 저장
    # json 형태로 /opt/airflow/dags/data/sensor_data_DAG수행날짜.json으로 저장 (json.dump(data,f))
    file_path = f'{DATA_PATH}/sensor_data_{kwargs['ds_nodash'] }.json'
    with open() as f:
        json.dump(data, f)

    pass

def _transform(**kwargs):
    #Xcom을 통해서 데이터를 df로 로드 섭씨를 화씨로 변환
    # 전처리된 건 csv로 덤핑

    pass

def _load(**kwargs):

    pass



with DAG(
    dag_id      = "05_mysql_etl",
    description = "ETL 수행하여 mysql에 결과물이 적재", #온도 센서 데이터 적재 가정
    default_args={
        'owner' : 'de_2team_manager',
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date = datetime(2026,2,25),
    catchup = False,
    tags= ['etl','mysql']
) as dag:
    # 3. task 정의
    """
    task_create_table = MySqlOperator(
        #최초는 생성, 존재하면 pass
        task_id ="create_table",
        mysql_conn_id = "mysql_default", # airflow 대시보드에서 사전 생성 admin-connections
        # sql
        sql = '''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

        '''
       

    )
     """
    task_extract = PythonOperator(
        task_id ="EXTRACT",
        python_callable= _extract
    )
    task_tranform = PythonOperator(
        task_id ="TRANSFORM",
        python_callable= _transform
    )

    task_load =PythonOperator(
        task_id ="LOAD",
        python_callable= _load
    )
   

    # 의존성 정의
    task_extract >> task_tranform >> task_load
    pass