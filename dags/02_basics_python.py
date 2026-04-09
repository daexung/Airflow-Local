'''
- PythonOperator 사용
- task 간 통신 => XCom 사용 => task간 상호 대화
'''
# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # 레벨별로 로그 출력 (에러, 경고, 디버깅, 정보..)

# 3-1. 콜백함수 정의
def _extract_cb(**kwargs):
    '''
    EXTRACT 담당 task 콜백 함수
    - params
        -kwargs : airflow가 작업 실행하기 전에 정보(airflow 내부 구성 context 접근 가능 내용)
    '''
    #1. airflow가 주입한 정보에서 필요한 정보 추출
    ti = kwargs['ti'] # ti : <TaskInstance:...> -- 대시보드 상에서 정사각형  박스
    exectue_date = kwargs['ds'] # 실행 고유 ID 
    run_id = kwargs['run_id'] 
        # 2. task 본연 업무 => 추출한 정보를 출력(로깅 활용)
    logging.info('== Extract 작업 start ==')
    logging.info(f'작업시간 {execute_date}, 실행 ID {run_id}')
    logging.info('== Extract 작업 end ==')

    # 3. XCom을 테스트를 위해서 특정 데이터를 반환 
    #    => XCom에 해당 데이터는 push됨(게시판에 글 등록됨)
    # 반환 행위 => 타 task에서 전달하는 행위로 활용될 수 있음
    return "Data Extract 성공"
    pass
def _transform_cb(**kwargs):
    ti = kwargs['ti']
    ti.xcom_pull(task_ids='extract_task_data')
    pass

# 2. DAG 정의
with DAG(
    dag_id            = '02_basics_python',
    description       = "파이썬 task 구성, Xcom통신",  
    default_args      = {
        'owner' : 'de_manager',
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1)
    }, 
    schedule_interval = '@daily', 
    start_date        = datetime(2026,2,25), 
    catchup           = False, 
    tags = ['python','xcom','context']
) as dag:
    
    # 3. TASK 정의 (PythonOperator 사용, XCom 사용)
    #    ETL을 고려하여 task 정의(간단)
    extract_task   = PythonOperator(
        task_id         = "extract_task_data",
        # 함수 단위(많은 작업을 하나의 단위로 구성)로 작업 구성 => 콜백함수 형태임
        python_callable = _extract_cb
    )
    transform_task = PythonOperator(
        task_id = "transform_task_data",
        python_callable = _transform_cb
    )

    # 4. 의존성 정의
    extract_task >> transform_task
    pass