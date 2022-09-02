import datetime
import pendulum
import numpy as np
import pandas as pd
import os
import json
import csv
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from airflow.contrib.operators import bigquery_to_gcs
from airflow import models
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from test1 import Total_incomee







default_args = {
    "owner": "noeal",
    "depends_on_past": False,
    "email": "nalbhnonesan@woolworths.com.au",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(
        year=2021,
        month=10,
        day=14,
        hour=0,
        minute=0,
        second=0,
        tzinfo=pendulum.timezone("Australia/Sydney"),
    ),
}



    
   

with models.DAG(
    dag_id="Python_File",
    default_view="graph",
    default_args=default_args,
    # retries=1,
    schedule_interval=None,
    catchup=False,
) as dag:



 
    Total_incomee = PythonOperator(
        task_id='Total_incomee',
        provide_context=True,
        python_callable=Total_incomee,
        dag=dag,
    )
    

email = EmailOperator(
        task_id='send_email',
        to=['nalbhnonesan@woolworths.com.au','mwood2@woolworths.com.au','mazam1@woolworths.com.au' , 'cnandigama1@woolworths.com.au'] ,
        subject='Airflow DAG for External Python Scripts',
        html_content="Total_incomeeeee  ",
        files=['/home/airflow/gcs/data/ti.csv'],
        provide_context = True,
        dag=dag
       
    )       
    
 
    
Total_incomee>>email