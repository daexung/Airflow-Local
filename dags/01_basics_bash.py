"""
- 기본 DAG 연습
- DAG 기본 형태가 갖춰지지 않으면 등록 x
- 구성이 갖춰지면 특정 시간이 지난 후 자동으로 등록

- 주제
    - bash 오퍼레이션 테스트, 적용코드 -> DAG 인식, 작동 확인
    - 확인 : 로그에서 확인 가능함.

- 특징
    - 현재 프로젝트 디렉토리는 docker 상에 특정 컨테이너와 연동되어 있음.
    - 작성한 코드들은 airflow 현재 생태계 내부로 공유됨 -> 인식됨 -> 대시보드에서 보임 : 볼륨설정


"""

# 1. 패키지, 모듈 가져오기
from airflow import DAG
# 오퍼레이터 2.xx 
from airflow.operators.bash import BashOperator
# 3.xx 에서는 airflow.providers.standard.operators.bash 사용함
# 시간,스케쥴, 계산등
from datetime import datetime, timedelta 
# DAG 정의에 필요한 파라미터를 외부에서 설정(옵션), DAG 내부에서도 가능
default_args = {
    'owner'             : 'de_2team_manaager', # DAG 소유주
    'depends_on_past'   : False , # 과거데이터 소급처리 (안함 설정)
    'retries'           : 1, # 작업 실패시 재시도 회수 (1회 설정)
    'retry_delay'       : timedelta(minutes=5), # 작업 실패 후 재시도까지 텀(5분 설정)
}
# 2. DAG 정의 -> 첫글자 대문자 -> 첫글자 대문자-> class로 이해 -> 객체 생성 시작됨
with DAG(
    dag_id      = "01_basics_bash", # DAG를 구분하는 용도
    description = "DE를 위한 ETL 핵심 airflow 연습용 DAG",  # DAG 설명
    default_args= default_args, # DAG의 기본 인자값
    schedule_interval = '@daily', #하루에 한 번 00:00분, cron식으로 표현 가능
    start_date = datetime(2026,2,25), # 현재 시점에서 시작일과의 차이를 고려하여 소급 처리여부체크
    catchup = False, # 과거에 대한 소급 처리 실행 방지 옵션
    tags = ['bash', 'basic'] #DAG 많으면 검색어려워서 태그 추가
) as dag:

    t1= BashOperator(
        task_id='date-print', #id 구성값:영문,숫자,하이픈,마침표,언더바로 구성
        bash_command = 'date', #리눅스 date 명령 # airflow의 지휘하에 작동되는 DAG구동 시 실제 할일을 구성하는 task 구분값
    )
    t2= BashOperator(
        task_id ='sleep',
        bash_command = 'sleep 5'
    )
    t3= BashOperator(
        task_id= 'echo-print',
        bash_command = 'echo "airflow task"'
    )
    # 시퀀스(구동 순서)
    # 의존성 정의
    # loop x, 방향성 가짐
    # t1이 실행되어야 ,t2가 실행
    # 대시보드에서 그래프에서 노드 형태로 확인 가능
    t1 >> t2 >> t3 
    pass