'''
함수 내부 연산결과 조건부로 task 진행
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule 
import logging
import random


def _branching(**kwargs):
    '''
        특정 조건에 따라 분기 처리해서 다음 수행 지정
    '''
    if random.choice([True,False]):
        logging.info('task process 실행')
        return "process" #이동하고 싶은 task
    else :
        logging.info('skip process 실행')
        return "skip"  


    pass


def _process(**kwargs):
    logging.info('특정 목적 실행')
    pass

with DAG(
    dag_id      = "04_basics_branching",
    description = "분기 처리, 선택적 TASK 구동",
    default_args={
        'owner' : 'de2teammanger',
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date = datetime(2026,2,25),
    catchup = False,
    tags= ['branch','trigger_rule']
) as dag:
    # 3. task 정의
    task_start = EmptyOperator(
            task_id="start",
            # jinja에서 값 출력 표현식 => {{변수|값|식|함수 등등}}
            
    )
    task_branch = BranchPythonOperator(
        task_id="branching",
        python_callable= _branching
    )
    task_process = PythonOperator(
        task_id="process",
        python_callable= _process
    )
    task_skip = EmptyOperator(
        task_id="skip"
    )
    task_end = EmptyOperator(
        task_id="end",
        # 최소 1개는 성공해야함
        trigger_rule= TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 4. 의존성 정의
    task_start >> task_branch
    task_branch >> task_process >> task_end
    task_branch >> task_skip >> task_end
    pass