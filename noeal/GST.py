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


sql= "SELECT *  FROM `gcp-wow-supers-rtlapsim-dev.test_intern.GST ON SALE1`"

with models.DAG(
    dag_id="GST",
    default_view="graph",
    default_args=default_args,
    # retries=1,
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    # ------------------------------------------------------------------------------------------------------------------
    ## landing start
    # ------------------------------------------------------------------------------------------------------------------

    GST_ON_SALE1 = BigQueryOperator(
        task_id="GST_ON_SALE1",
        sql=sql,
        use_legacy_sql=False,
        destination_dataset_table="gcp-wow-supers-rtlapsim-dev.test_intern.Table6",
        write_disposition="WRITE_TRUNCATE",
        
        
    )
    
    export_GST_ON_SALE1_to_gcs= bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_insight_indicator_dow_to_gcs',
        source_project_dataset_table="gcp-wow-supers-rtlapsim-dev.test_intern.Table6",
        destination_cloud_storage_uris='gs://us-central1-rtla-dev-v2-bacbcff2-bucket/data/GST.CSV',
        export_format='CSV'
    )
    
    
      
    email_summary =EmailOperator(
        task_id='email_summary',
        to=['nalbhnonesan@woolworths.com.au','mwood2@woolworths.com.au','mazam1@woolworths.com.au' , 'cnandigama1@woolworths.com.au'] ,
        subject='Send Email CSV_File',
        html_content="Gst on Sale From Sql ",
        files=['/home/airflow/gcs/data/GST.CSV']
        )
        

GST_ON_SALE1 >> export_GST_ON_SALE1_to_gcs>>email_summary