'''
- macro +jinja 활용하여 airflow 내부 정보 접근 출력 등
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging


# 2. DAG 정의
with DAG() as dag:
    # task 정의
    t1 = BashOperator()
    t2 = BashOperator()
    t3 = PythonOperator()

    # 의존성 정의
    t1 >> t2 >> t3
    pass