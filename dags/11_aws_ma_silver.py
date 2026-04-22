'''
- ma 에서 silver 단계 처리
- 스케줄 ( 10 * * * * )
- 데이터 (flatten, 파생변수, 컬럼명변경) 전처리 수행(sql을 통해)
    - event_id
    - event_time => event_timestamp
    - data.user_id
    - data.item_id
    - data.price
    - data.qty
    - (data.price * data.qty) as total_price 
    - data.store_id
    - source_ip
    - user_agent
    - dt (year-month-day)
    - hour as hr
- 작업 (silver 테이블 삭제 -> ctas)
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
# 2. 환경변수

DATABASE_BRONZE = 'de-ai-19-ma-bronze-db'
DATABASE_SILVER = 'de-ai-30-ma-silver-db'
SILVER_S3_PATH = 's3://de-ai-18-827913617635-ap-northeast-2-an/medallion/silver/'
ATHENA_RESULTS = 's3://de-ai-18-827913617635-ap-northeast-2-an/athena-results/'
SILVER_TBL_NAME = 'sales_silver_tbl'
# 3. DAG 정의
# 3. DAG 정의
with DAG(
    dag_id      = "11_medallion_bronze_to_silver_ctas", 
    description = "athena ctas 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '19 * * * *', # 매 시 10분에 실행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'silver','athena', 'ctas'],
) as dag:
    # 4. task 정의 (2개)
    drop_silver_task = AthenaOperator(
        task_id= 'drop_silver_tbl',
        query ='drop table if  exists {{ params.database_silver }}.{{params.tbl_nm}}', # 진자로 표현
        database = DATABASE_SILVER ,
        output_location = ATHENA_RESULTS,
        params = {'database_silver': DATABASE_SILVER, 'tbl_nm':SILVER_TBL_NAME}
    )
    ctas_silver_task = AthenaOperator(
        task_id = 'ctas_silver',
        query   = '''
            Create Table if not exists {{ params.database_silver }}.{{ params.tbl_nm }};
            with (

            ) As 
            Select 
                event_id
                event_time => event_timestamp,
                data.user_id,
                data.item_id,
                data.price,
                data.qty,
                (data.price * data.qty) as total_price, 
                data.store_id,
                source_ip,
                user_agent,
                cast(year || '-' || month || '-' ||day as VARCHAR) as dt,
                hour as hr, 
              
            from {{ params.DATABASE_BRONZE }}.raw_bronze_tbl
            where   year = {{ execution_date.foramt('YYYY') }}
                and month= {{ execution_date.foramt('MM') }}
                and day  = {{ execution_date.foramt('DD') }}
                and hour = {{ execution_date.foramt('HH') }}

        ''',  # 이 위는 브론즈 데이터에서 내가 원하는걸 빼오는 것
        database= DATABASE_SILVER,
        params  = {
            'database_bronze':DATABASE_BRONZE, 
            'database_silver':DATABASE_SILVER, 
            'tbl_nm':SILVER_TBL_NAME
        } 
    )
    # 5. 의존성 구성
    drop_silver_task >> ctas_silver_task