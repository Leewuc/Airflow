# Airflow Practice Repository

이 저장소는 Apache Airflow 연습을 위한 예제 DAG들과 도커 컴포즈 환경을 포함합니다. 로컬에서 간단히 Airflow를 실행하고, DAG 작성 및 커스텀 플러그인 사용법 등을 실습할 수 있습니다.

## 사전 준비

- [Docker](https://www.docker.com/) 및 [docker-compose](https://docs.docker.com/compose/) 설치가 필요합니다.
- 현재 디렉터리의 `.env` 파일에서 Airflow가 사용할 UID를 지정합니다.

```dotenv
AIRFLOW_UID=1000
```

## 실행 방법

1. 저장소를 클론한 뒤 `docker-compose up -d` 명령으로 컨테이너를 실행합니다.
2. 기본 웹서버 주소는 `http://localhost:8080` 이며 초기 계정은 `airflow / airflow` 입니다.
3. 종료 시에는 `docker-compose down` 을 실행합니다.

컨테이너에서는 Postgres, Redis, CeleryExecutor가 함께 기동되며, 필요 시 `docker-compose.yaml` 파일을 수정하여 설정을 변경할 수 있습니다.

## 주요 폴더 설명

- `dags/` : 여러 가지 예제 DAG 파일이 위치합니다.
- `plugins/` : 사용자 정의 플러그인과 스크립트가 들어 있습니다.
- `logs/` : Airflow 로그가 저장되는 위치입니다.

모든 폴더는 도커 볼륨으로 매핑되므로 컨테이너 재시작 시에도 변경 사항이 유지됩니다.

## 예제 DAG

`dags` 폴더에는 BashOperator, PythonOperator 등 다양한 예제가 포함되어 있습니다. 다음은 변수 사용 예제를 보여주는 `dags_bash_with_variable.py` 의 일부입니다.

```python
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 6, 17, tz="Asia/Seoul"),
    catchup=False
) as dag:
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable:{var_value}"
    )

    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command="echo variable:{{var.value.sample_key}}"
    )
```

추가로, `plugins/operators/seoul_api_to_csv_operator.py` 에서는 서울시 공공데이터 API를 호출하여 CSV로 저장하는 연산자를 정의합니다.

```python
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd


class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
```

## 커스텀 플러그인 예시

`plugins/common/common_func.py` 파일에는 간단한 공통 함수 예제가 들어 있습니다.

```python
def get_sftp():
    print('sftp 작업을 시작합니다.')

def regist(name, sex, *args):
    print(f"이름: {name}")
    print(f"성별: {sex}")
    print(f"기타옵션들: {args}")

def regist2(name, sex, *args, **kwargs):
    print(f"이름: {name}")
    print(f"성별: {sex}")
    print(f"기타옵션들: {args}")
    email = kwargs['email'] or 'empty'
    phone = kwargs['phone'] or 'empty'
    if email:
        print(email)
    if phone:
        print(phone)
```

## 참고

`docker-compose.yaml` 파일에는 Airflow 이미지, 데이터베이스, Redis 설정 등이 정의되어 있습니다. 필요에 따라 포트 및 환경 변수를 수정하여 사용할 수 있습니다.

Airflow에 대한 자세한 내용은 [공식 문서](https://airflow.apache.org/)를 참고하세요.
