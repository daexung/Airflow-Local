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
    file_path = f"{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json"
    with open(file_path, 'w') as f:
        json.dump(data, f)
    logging.info(f'extract 한 로그 데이터{file_path}') # Xcom을 통해서 task_transform에게 로그의 경로를 전달해야함( 실데이터는 x)
    return file_path

    pass

def _transform(**kwargs):
    #Xcom을 통해서 데이터를 df로 로드 섭씨를 화씨로 변환
    # 전처리된 건 csv로 덤핑
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='EXTRACT')
    #로그 출력
    logging.info(f'전달받은데이터 {json_file_path}')
    df = pd.read_json(json_file_path)
    # 설정: 공장에서 섭씨 100도미만 정상 데이터로 간주한다. 100도 이상 데이터는 기계 오작동
    # 1. 100도 미만 데이터만 추출 판다스의 블리언 인덱싱 사용
    target_df = df[df['temperature'] < 100].copy()
    # 2. 파생변수로 화씨 데이터 구성 화씨 = 섭씨 *9/5 +32
    target_df['temperature_f'] = (target_df['temperature']* 9 / 5) +32
    file_path = f"{DATA_PATH}/preprocessing_data_{kwargs['ds_nodash']}.csv"
    target_df.to_csv(file_path, index=False) #인덱스 제외
    logging.info(f'전처리 후 csv 저장 완료 {file_path}')
    return file_path

    



    pass

def _load(**kwargs):
    #csv 경로 획득 -> csv -> df -> mysql 연결 mysqlhook사용 커서를 획득해 insert 구문, 커밋 연결종료 전체를 try except로 감싸기 (I/O)
    ti=kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='TRANSFORM')

    df = pd.read_csv(csv_path)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn - mysql_hook.get_conn()

    try:
        with conn.cursor() as cursor:
            sql = '''
            insert into sensor_readings
            (sensor_id, timestamp, temperature_c, temperature_f)
            values (%s, %s, %s, %s)

            '''
            params = [
                ( data['sensor_id'], data['timestamp'], data['temperature'], data['temperature_f'])
                for _ , data in df.iterrows()
            ]
            logging.info(f'입력할 데이터(파라미터) {params}')
            cursor.executemany(sql,params)
            conn.commit()
            logging.info('mysql에 적제 완료')

            pass
    except Exception as e:
        logging.info(f'적제 오류 {e}')  #예외 던지기로 변경 필요(리뷰 때 시도)
        pass
    finally:
        if conn:
            conn.close()
            logging.info(f'mysql 연결 종료(뒷정리)')
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
    task_create_table >> task_extract >> task_tranform >> task_load
    pass