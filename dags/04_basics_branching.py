'''
함수 내부 연산결과 조건부로 task 진행
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule 
import logging


with DAG(
    dag_id      = "03_basic_macro_jinja",
    description = "macro를 통해 context 접근, jinja를 통해 표현 ",
    default_args={
        'owner' : 'de2teammanger',
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1)
    },
    schedule_interval = '0 9 * * *',
    start_date = datetime(2026,2,25),
    catchup = False,
    tags= ['jinja','macro','context']
) as dag:
    # 3. task 정의
    task_start = EmptyOperator(
            task_id="jinja_used_bash",
            # jinja에서 값 출력 표현식 => {{변수|값|식|함수 등등}}
            bash_command= "echo 'DAG t1 task 수행시간 {{ds}},{{ds_nodash}}'",
    )
    task_branch = BranchPythonOperator(
        task_id="jinja_macro_bash",
        bash_command= "echo '일주일전 수행시감 계산 {{macros.ds_add(ds,-7)}}'",
    )
    task_process = PythonOperator(
        task_id="jinja_used_python",
        python_callable= _print
    )
    task_skip = EmptyOperator()
    task_end = EmptyOperator()

    # 4. 의존성 정의
    task_start >> task_branch
    task_branch >> task_process >> task_end
    task_branch >> task_skip >> task_end
    pass