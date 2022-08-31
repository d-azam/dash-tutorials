import datetime
from pydoc import importfile
import pendulum
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators import bigquery_to_gcs
from night_fill.night_fill_python_code import todo
from night_fill.night_fill_python_code import total_amount


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
                                    month=8,
                                    day=1,
                                    hour=0,
                                    minute=0,
                                    second=0,
                                    tzinfo=pendulum.timezone("Australia/Sydney")
                                    )
}

sql = """SELECT site, article,cast(Sales_ExclTax as int64) as tot_qty, CAST(RAND()*20 AS Int64) as pallet
    FROM `gcp-wow-supers-rtlapsim-dev.test_intern.fin_smkt_profit`
    WHERE Calendar_Day = '2022-06-01'
    and Sales_ExclTax > 10
    order by pallet DESC"""


email_list = ['sbarkil@woolworths.com.au', 'mwood2@woolworths.com.au' ,'cnandigama1@woolworths.com.au' , 'mazam1@woolworths.com.au']

result_email_body = """Hi everyone, <br>  I'm sending this email after I separate the Python code from the Dag. <br> The total time required to complete the night fill work is <b> {{ ti.xcom_pull(task_ids='night_fill_time_calc') }} </b> minutes.<br>
Also, The total number of articles in this night fill work is <b> {{ ti.xcom_pull(task_ids='night_fill_total_amount') }}. </b>"""


with models.DAG(
    dag_id="nightfill_run_from_file",
    default_args=default_args,
    # retries=1,
    schedule_interval=None,
    catchup=False,
    ) as dag:

        night_fill_data = BigQueryOperator(
        task_id="night_fill_data",
        sql=sql,
        use_legacy_sql=False,
        destination_dataset_table="gcp-wow-supers-rtlapsim-dev.test_intern.Table5",
        write_disposition="WRITE_TRUNCATE",
    )
	
        export_night_fill_data_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_night_fill_data_to_gcs',
        source_project_dataset_table = 'gcp-wow-supers-rtlapsim-dev.test_intern.Table5',
        destination_cloud_storage_uris = 'gs://us-central1-rtla-dev-v2-bacbcff2-bucket/data/night_fill.csv',
        export_format='CSV',
    )

        

        night_fill_time_calc = PythonOperator(
        task_id = "night_fill_time_calc",
        python_callable = todo, 
        provide_context = True,
        dag = dag,
    )

        night_fill_total_amount = PythonOperator(
        task_id = "night_fill_total_amount",
        python_callable = total_amount, 
        provide_context = True,
        dag = dag,
    )
        send_email_result = EmailOperator(
        task_id = 'send_email_result',
        to = email_list,
        subject = 'The night fill required time',
        html_content = result_email_body,
        provide_context = True,
        dag = dag,
    )

night_fill_data >> export_night_fill_data_to_gcs >> night_fill_time_calc >> night_fill_total_amount >> send_email_result
