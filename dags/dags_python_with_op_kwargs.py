from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from plugins.common.common_func import regist2
with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 6, 14, tz="Asia/Seoul"),
    catchup=False
) as dag:

    regist2_t1 = PythonOperator(
        task_id="regist2_t1",
        python_callable=regist2,
        op_args=['LeeU','man','kr','seoul'],
        op_kwargs={'email':'0522hyung@gmail.com','phone':'010'}
    )

    regist2_t1