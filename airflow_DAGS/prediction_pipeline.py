"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from datetime import timedelta
from datetime import datetime
from airflow.operators.email_operator import EmailOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 8, 30, 0, 0),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    'prediction_pipeline',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='echo',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    #priority_weight=2**31-1,
    priority_weight=1)

t2 = BashOperator(
    task_id='Data_Ingestion',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    #priority_weight=2**31-1,
    priority_weight=1)


t3 = BigQueryCheckOperator(
    task_id='data_validation',
    sql='''
    #legacySql
    SELECT COUNT(pageviews) 
    FROM [jsb-demos.ga_360_merch_store.merch_store_ga360_sessions] LIMIT 1
    ''',
    dag=dag,
    depends_on_past=True,
    ui_color= '#bed578' )


t4 = BigQueryCheckOperator(
    task_id='key_metrics_comparison',
    sql='''
    #legacySql
    SELECT
    STRFTIME_UTC_USEC(timestamp, "%Y%m%d") as date
    FROM
      [bigquery-public-data:hacker_news.full]
    WHERE
    type = 'story'
    AND STRFTIME_UTC_USEC(timestamp, "%Y%m%d") = "{{ yesterday_ds_nodash }}"
    LIMIT 1
    ''',
    dag=dag,
    ui_color= '#0050aa')



t5 = BashOperator(
    task_id='Data_Transformation',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    #priority_weight=2**31-1,
    priority_weight=1)

t6 = BashOperator(
    task_id='Feature_Validation',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    #priority_weight=2**31-1,
    priority_weight=1)

t7 = BashOperator(
    task_id='Run_Predictions',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    priority_weight=1)


t8 = BashOperator(
    task_id='Apply_Business_Rules',
    bash_command='echo test',
    dag=dag,
    depends_on_past=True,
    priority_weight=1,
    ui_color= '#0050aa')

t9 = BashOperator(
    task_id='Generate_Reports',
    bash_command='echo test',
    dag=dag,
    depends_on_past=True,
    priority_weight=1)





e1 = EmailOperator(
    task_id="send_mail", 
    to='jasmeetsb1@gmail.com',
    subject='Data ingestion completed',
    html_content='<p> You have got mail! <p>',
    dag=dag)


e2 = EmailOperator(
    task_id="Email_Report", 
    to='jasmeetsb1@gmail.com',
    subject='Data validation completed',
    html_content='<p> You have got mail! <p>',
    dag=dag)

t1 >> t2 >> t3 >> t5 >> t6 >> t7 >> t8 >> t9
t2 >> e1
t9 >> e2
t3 >> t4 


