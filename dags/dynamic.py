import airflow 
import json
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


args = {
    'owner': 'Faqih',
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['abdullah.mubarok@tokopedia.com', 'rubila.adawiyah@tokopedia.com', 'lulu.sundayana@tokopedia.com'],
    'email_on_failure': True,
    'provide_context': True,
}

dag = DAG(
    dag_id='pipeline2',
    default_args=args,
    schedule_interval=timedelta(minutes=1),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1, 
    catchup=False
)

def print_var(**kwargs):
    print(Variable)

task = PythonOperator(
    dag=dag,
    task_id='globals',   
    python_callable=print_var
)


# args = {
#     'owner': 'Faqih',
# }

# def create_dag(pipeline):
#     dag = DAG(
#         dag_id=pipeline['dag']['dag_name'],
#         default_args=pipeline['dag']['args'],
#         schedule_interval=pipeline['dag']['schedule_interval'],
#         start_date= airflow.utils.dates.days_ago(2)
#         )
#     def hello():
#         print("Hello")

#     with dag: 
#         task = PythonOperator(
#             task_id='task1',
#             dag=dag, 
#             python_callable=hello,
#             max_active_runs=1, 
#             cathcup=False
#         )
    
#     return dag

# with open('dags/dum_req.json') as f:
#     request = json.load(f)

# for pipeline, values in request.items():
#     dag_id = values['dag']['dag_name']
#     globals()[dag_id]=create_dag(values)
