'''
- 로그 데이터 발생 ->오픈서치 -> 직접 전송
- pip install -1 opensearch-py
'''

#1 모듈 가져오기

from opensearchpy import OpenSearch
from datetime import datetime
import random 
import time