# 개요
    - fastapi를 이용한 API 서비스 간단하게 구성

# 구동
    main:app --host 0.0.0.0 --port 8000

# 접속
    - 홈페이지
        - http://127.0.0.1:8000/
    - 문서 접속(swagger docs)
        - swaggerr를 fastapi 기본제공(flask, django 기본 x)
        - - http://127.0.0.1:8000/docs
        '''
            api 문서 제공, api 테스트 기능
        '''
    - api 문서 간결화된
        - - http://127.0.0.1:8000/redoc
    - api 테스트 전용
        - postman (구글 익스텐션에서 출발해서 독립함)
        - thunderclient (vscode extension)