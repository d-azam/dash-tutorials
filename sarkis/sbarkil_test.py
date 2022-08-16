import datetime

import pendulum
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from airflow.contrib.operators import bigquery_to_gcs

from airflow import models

default_args = {
    "owner": "sbarkil",
    "depends_on_past": False,
    "email": "sbarkil@woolworths.com.au",
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


sql = "SELECT * FROM  gcp-wow-supers-rtlapsim-dev.test_intern.high_selling_v "

success_email_body = f"""
Hi, <br><br>
process_incoming_files DAG has been executed successfully .
"""
result_email_body = f"""
Hi,<br><br>
please find attached file for the result.
"""

with models.DAG(
    dag_id="sbarkil_test",
    default_args=default_args,
    # retries=1,
    schedule_interval=None,
    catchup=False,
) as dag:

    #start = DummyOperator(task_id="start")

    # ------------------------------------------------------------------------------------------------------------------
    # landing start
    # ------------------------------------------------------------------------------------------------------------------

    high_selling = BigQueryOperator(
        task_id="high_selling",
        sql=sql,
        use_legacy_sql=False,
        destination_dataset_table="gcp-wow-supers-rtlapsim-dev.test_intern.Table2",
        write_disposition="WRITE_TRUNCATE",
    )
    
    #Send Email on succes
    
    #send_mail = EmailOperator(
        #task_id="send_mail", 
        #to = "sbarkil@woolworths.com.au",
        #subject='Airflow Success: sbarkil_test',
        #html_content=success_email_body,
        #dag=dag
    #)
    
    #Export the result table as CSV
    
    export_high_selling_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_high_selling_to_gcs',
        source_project_dataset_table = 'gcp-wow-supers-rtlapsim-dev.test_intern.Table2',
        destination_cloud_storage_uris = 'gs://us-central1-rtla-dev-v2-bacbcff2-bucket/data/high_selling.csv',
        export_format='CSV'
    )
    
    #Send email with the result table 
    
    send_mail_result = EmailOperator(
        task_id="send_mail_result", 
        to = "sbarkil@woolworths.com.au",
        subject='High selling table',
        html_content = result_email_body,
        files=['/home/airflow/gcs/data/high_selling.csv']
        )
        
    
high_selling >> export_high_selling_to_gcs >> send_mail_result