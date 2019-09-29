import airflow
import io
import pandas as pd
import json
import pickle
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook 
from google.cloud import bigquery, storage
from datetime import timedelta, datetime
import modules.forecast_prophet as prophet

with open('dags/request.json') as f:
    request = json.load(f)

DAG_NAME = request['DAG_NAME']
SCHEDULE_INTERVAL = request['SCHEDULE_INTERVAL'] 
BQ_CONN_ID = request['BQ_CONN_ID']
BQ_PROJECT_DESTINATION = request['BQ_PROJECT_DESTINATION']
BQ_DATASET_DESTINATION = request['BQ_DATASET_DESTINATION']
BQ_TABLE_DESTINATION = request['BQ_TABLE_DESTINATION']
BUCKET_DESTINATION = request['BUCKET_DESTINATION']
FOLDER_IN_BUCKET = request['FOLDER_IN_BUCKET']
IS_USING_ML = request['IS_USING_ML']
QUERY = request['QUERY'] 
CATEGORY = request['CATEGORY']
TARGET_FORECAST = request['TARGET_FORECAST']

# Connection Hook
bq_hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
storage_client = storage.Client(project = bq_hook._get_field("project"), credentials = bq_hook._get_credentials())
bq_client = bigquery.Client(project = bq_hook._get_field("project"), credentials = bq_hook._get_credentials())

args = {
    'owner': 'Faqih',
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['abdullah.mubarok@tokopedia.com', 'rubila.adawiyah@tokopedia.com', 'lulu.sundayana@tokopedia.com'],
    'email_on_failure': True,
    'provide_context': True,
    
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    start_date=datetime(2018, 8, 1),
    schedule_interval=timedelta(days=SCHEDULE_INTERVAL),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1, 
    catchup=False
)

def if_table_exists(**kwargs):
    bq_table = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+'*')
    #read file 
    try:
        bq_client.get_table(bq_table)
        kwargs['ti'].xcom_push(key='created_flag', value=True)
    except:
        kwargs['ti'].xcom_push(key='created_flag', value=False)

def branch_table(**kwargs):
    is_table_exists = kwargs['ti'].xcom_pull(key='created_flag', task_ids='chck_table')
    print('is_table_exists' + str(is_table_exists))
    if not IS_USING_ML:
        return 'mail'
    elif IS_USING_ML and is_table_exists: 
        return 'predict'
    else:
        return 'train'

def to_table(**kwargs):
    is_table_exists=kwargs['ti'].xcom_pull(key='created_flag', task_ids='chck_table')

    #table_ref = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+str(kwargs['execution_date'].date().strftime('%Y%m%d')))
    if is_table_exists:
        table_ref = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+'20180914')
    else:
        table_ref = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+'20180831')


    job_config = bigquery.QueryJobConfig()
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.write_disposition = 'WRITE_TRUNCATE'    
    job_config.destination = table_ref
    # -- Uncomment to create partitioned table
    #job_config.time_partitioning = bigquery.table.TimePartitioning()
    
    #is_table_exists = False
    if is_table_exists:
        #sql=QUERY.format('=',str(kwargs['execution_date'].date()))
        sql=QUERY.format('2018-09-19 00:00:00','2018-09-19 23:59:59')
    else:
        #sql=QUERY.format('<=',str(kwargs['execution_date'].date()))
        sql=QUERY.format('<=','2018-08-31 00:00:00')

    query_job = bq_client.query(
        sql,
        location='US',
        job_config=job_config,
    )
    result = query_job.result()
    kwargs['ti'].xcom_push(key='row_num', value=result.total_rows)

def query_to_csv(**kwargs):    
    #table_ref = bq_client.dataset(dataset_id=BQ_DATASET_DESTINATION).table(table_id=BQ_TABLE_DESTINATION+'_'+str(kwargs['execution_date'].date().strftime('%Y%m%d')))

    is_table_exists=kwargs['ti'].xcom_pull(key='created_flag', task_ids='chck_table')

    #table_ref = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+str(kwargs['execution_date'].date().strftime('%Y%m%d')))
    if is_table_exists:
        table_ref = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+'20180914')
        uri = 'gs://'+BUCKET_DESTINATION+'/'+FOLDER_IN_BUCKET+BQ_TABLE_DESTINATION+'_'+'20180914.csv'
    else:
        table_ref = bq_client.dataset(BQ_DATASET_DESTINATION).table(BQ_TABLE_DESTINATION+'_'+'20180831')
        uri = 'gs://'+BUCKET_DESTINATION+'/'+FOLDER_IN_BUCKET+BQ_TABLE_DESTINATION+'_'+'20180831.csv'

    csv_job = bq_client.extract_table(
        source = table_ref,
        #Change the destination uri if needed
        # 'gs://'+BUCKET_DESTINATION+'/'+FOLDER_IN_BUCKET+BQ_TABLE_DESTINATION+'_'+str(kwargs['execution_date'].date().strftime('%Y%m%d'))+'.csv' 
        destination_uris = uri,
        location='US',
    )
    csv_job.result()

def gcs_csv_to_df(bq_hook, execution_date):
    #todo : change the blob folder to be filled with the request.json 
    #blob = storage_client.get_bucket(BUCKET_DESTINATION).get_blob(FOLDER_IN_BUCKET+BQ_TABLE_DESTINATION+'_'+str(kwargs['execution_date'].date().strftime('%Y%m%d'))+'.csv') 
    blob = storage_client.get_bucket(BUCKET_DESTINATION).get_blob(FOLDER_IN_BUCKET+BQ_TABLE_DESTINATION+'_'+execution_date+'.csv') 
    byte_stream = io.BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    df = pd.read_csv(byte_stream)
    return df

def save_to_gcs(bq_hook, stream, path_file):
    blob = storage_client.get_bucket(BUCKET_DESTINATION).blob(FOLDER_IN_BUCKET+path_file)
    byte_stream = io.BytesIO()
    pickle.dump(stream, byte_stream, pickle.HIGHEST_PROTOCOL)
    byte_stream.seek(0)
    blob.upload_from_file(byte_stream)

def train_mdl(**kwargs):
    #df = gcs_csv_to_df(bq_hook, kwargs['execution_date'].date())
    df = gcs_csv_to_df(bq_hook, '20180831')

    df.rename(columns={TARGET_FORECAST: 'y'})
    model = prophet.train(df, category_cols=CATEGORY)
    save_to_gcs(bq_hook,  model, 'model/model.pickle')
    
    #prediction = predict_mdl(kwargs['execution_date'].date())
    prediction = predict_mdl(datetime(2018,8,31).date())

    prediction.to_csv('prediction.csv', index=False)

    #blob = storage_client.get_bucket(BUCKET_DESTINATION).blob(FOLDER_IN_BUCKET+'prediction/prediction_'+str((kwargs['execution_date']+timedelta(days=1)).date().strftime('%Y%m%d'))+'.csv')
    blob = storage_client.get_bucket(BUCKET_DESTINATION).blob(FOLDER_IN_BUCKET+'prediction/prediction_20180831'+'.csv')

    blob.upload_from_filename('prediction.csv')
    return prediction

def predict_mdl(execution_date):
    blob = storage_client.get_bucket(BUCKET_DESTINATION).get_blob(FOLDER_IN_BUCKET+'model/model.pickle')
    byte_stream = io.BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    models = pickle.load(byte_stream)
    result = prophet.predict(execution_date, models=models, schedule_interval=SCHEDULE_INTERVAL, category_cols=CATEGORY)
    return result

def predict(**kwargs):
    #prediction = predict_mdl(kwargs['execution_date'].date())
    prediction = predict_mdl(datetime(2018,9,14).date())

    prediction.to_csv('prediction.csv', index=False)
    
    #blob = storage_client.get_bucket(BUCKET_DESTINATION).blob(FOLDER_IN_BUCKET+'prediction/prediction_'+str((kwargs['execution_date']+timedelta(days=1)).date().strftime('%Y%m%d'))+'.csv')
    blob = storage_client.get_bucket(BUCKET_DESTINATION).blob(FOLDER_IN_BUCKET+'prediction/prediction_20180914'+'.csv')
    blob.upload_from_filename('prediction.csv')
    return prediction

chck_table = PythonOperator(
    task_id = 'chck_table',
    dag=dag,
    python_callable=if_table_exists,
)

chck_table_branch = BranchPythonOperator(
    task_id = 'chck_table_branch',
    dag=dag,
    python_callable=branch_table,
)

crt_table = PythonOperator(
    task_id='crt_table',
    dag=dag,
    python_callable=to_table
)

save_to_csv = PythonOperator(
    task_id='save_to_csv',
    dag=dag,
    python_callable=query_to_csv
)

train = PythonOperator(
    task_id='train',
    dag=dag,
    python_callable=train_mdl,
)

mail = EmailOperator(
    task_id='mail',
    dag=dag,
    trigger_rule='none_failed',
    to='abdullah.mubarok@tokopedia.com',
    subject='Reporting: Final Project {{ ds }}',
    params={
        'dag_name': DAG_NAME,
        'table': BQ_TABLE_DESTINATION, 
        'dataset': BQ_DATASET_DESTINATION,
        'project': BQ_PROJECT_DESTINATION,
        'bucket': BUCKET_DESTINATION
        },
    html_content=''' 
    DAG name : {{ params.dag_name }}
    <br>
    Table : {{ params.table }}
    <br>
    Dataset : {{ params.dataset }} 
    <br>
    Project : {{ params.project }}
    <br>
    CSV link in GCS : https://storage.cloud.google.com/{{ params.bucket }}/{{ params.dataset }}/{{ params.table }}{{ds_nodash}}.csv
    <br>
    Number of recorded rows : {{task_instance.xcom_pull(task_ids='crt_table', key='row_num')}}
    ''',
    files = ["dags/tooth.jpg"],
    cc=['rubila.adawiyah@tokopedia.com', 'lulu.sundayana@tokopedia.com'],
)

predict = PythonOperator(
    task_id='predict',
    dag=dag,
    python_callable=predict,
)

chck_table >> crt_table >> save_to_csv >> chck_table_branch >> [train, predict, mail] 
train >> mail 
predict >> mail 
