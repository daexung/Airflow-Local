'''
- 주가 로그 생성기, boto3 를 이용하여 직접 연계
- 로그 > 키네시스로 전달
'''

import time
import random
import json
from datetime import date
import boto3