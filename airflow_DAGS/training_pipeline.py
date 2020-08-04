
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from datetime import timedelta
from datetime import datetime
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import numpy as np

from google.cloud import bigquery


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
    'training_pipeline',
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


##########

#Authentication
client = bigquery.Client()
#client = bigquery.Client.from_service_account_json('<Add PATH to Service Account key')


#Retrieve data from BQ
sql = """
SELECT * FROM `jsb-demos.yellow_taxi.yellow_taxi_fare_2m` 
#WHERE pickup_datetime < '2016-01-03'
ORDER BY pickup_datetime 
LIMIT 10000
"""
#Convert to DataFrame
train_data = client.query(sql).to_dataframe()

#Random Split data into two sets. 
#Dummy step. In actual deployment assign two data sets to be validated to df1 and df2
msk = np.random.rand(len(train_data)) < 0.5
df1 = train_data[msk]
df2 = train_data[~msk]


#Define function to compare two datasets and calculate deltas in key stats

def evaluate_drift(old_data=df1, new_data=df2):
    df1 = old_data
    df2 = new_data

    #Calculate key stats
    df1_sts = df1.describe()
    df2_sts = df2.describe()

    rows = df1_sts.index.values
    columns = df1_sts.columns.values
    
    #Append blank fields
    df1_sts = df1_sts.append(pd.Series(name='skew',dtype="float64"))
    df1_sts = df1_sts.append(pd.Series(name='kurt',dtype="float64"))
    df1_sts = df1_sts.append(pd.Series(name='percent_missing',dtype="float64"))
    
    df2_sts = df2_sts.append(pd.Series(name='skew',dtype="float64"))
    df2_sts = df2_sts.append(pd.Series(name='kurt',dtype="float64"))
    df2_sts = df2_sts.append(pd.Series(name='percent_missing',dtype="float64"))

    #Calculate skew, kurt, missing% for each field
    for col in columns:
        df1_sts[col]['skew'] = df1[col].skew()
        df1_sts[col]['kurt'] = df1[col].kurt()
        df1_sts[col]['percent_missing'] = df1[col].isnull().sum()/len(df1)*100
        
        df2_sts[col]['skew'] = df2[col].skew()
        df2_sts[col]['kurt'] = df2[col].kurt()
        df2_sts[col]['percent_missing'] = df2[col].isnull().sum()/len(df2)*100
        


    rows = df1_sts.index.values
    columns = df1_sts.columns.values
    drift_summary = pd.DataFrame(columns=columns, index=rows)

    #Calculate the delta(%) for each field between the two datasets
    for col in columns:
        for row in rows:
            if (df1_sts[col][row] != 0):
                drift_value = (df1_sts[col][row] - df2_sts[col][row])/df1_sts[col][row]
            elif (df2_sts[col][row] != 0):
                drift_value= (df1_sts[col][row] - df2_sts[col][row])/df2_sts[col][row]
            else:
                drift_value = 0


            drift_summary[col][row] = abs(round(drift_value, 2))

    return(df1_sts,df2_sts, drift_summary)



df1_sts, df2_sts, drift_summary = evaluate_drift(df1,df2)
print(drift_summary)


##########


p1 = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=evaluate_drift,
    dag=dag,
)


t3 = BigQueryCheckOperator(
    task_id='bq_check_table',
    sql='''
    #legacySql
    SELECT COUNT(pageviews) 
    FROM [jsb-demos.ga_360_merch_store.merch_store_ga360_sessions] LIMIT 1
    ''',
    dag=dag,
    depends_on_past=True,
    ui_color= '#bed578' )


t4 = BigQueryCheckOperator(
    task_id='bq_data_validation',
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
    task_id='Feature_Creation',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    #priority_weight=2**31-1,
    priority_weight=1)

t7 = BashOperator(
    task_id='Train_Model',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    priority_weight=1)


t8 = BashOperator(
    task_id='Validate_Model_Performance',
    bash_command='echo test',
    dag=dag,
    depends_on_past=True,
    priority_weight=1,
    ui_color= '#0050aa')

t9 = BashOperator(
    task_id='Deploy_Model',
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
    task_id="send_mail_2", 
    to='jasmeetsb1@gmail.com',
    subject='Data validation completed',
    html_content='<p> You have got mail! <p>',
    dag=dag)

t1 >> t2 >> t3 >> t5 >> t6 >> t7 >> t8 >> t9
t2 >> e1
t2 >> p1
t3 >> e2
t3 >> t4 


#default_args = { 
#   'start_date': datetime(2019, 8, 30, 0, 0), 'retries': 0,
#    'retry_delay': timedelta(minutes=5), 'project_id': cfg.PROJECT_ID,
#    'region': cfg.REGION}
#sql = """   SELECT commit, subject, message
#FROM bigquery-public-data.github_repos.commits LIMIT 1000 """,
