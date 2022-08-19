import datetime
import pendulum
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators import bigquery_to_gcs
import pandas as pd

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

path ="/home/airflow/gcs/data/night_fill.csv"

email_list = ['sbarkil@woolworths.com.au', 'mwood2@woolworths.com.au' ,'cnandigama1@woolworths.com.au' , 'mazam1@woolworths.com.au']

result_email_body = """The total time required to complete the night fill work is <b> {{ ti.xcom_pull(task_ids='night_fill_time_calc') }} </b> minutes.<br>
Also, The total number of articles in this night fill work is <b> {{ ti.xcom_pull(task_ids='night_fill_total_amount') }}. </b>"""


class NightFill():
    def __init__(self, df, num_aisles):
        self.name = 'Sarkis Barkil'
        self.load_rate_article = 30/400 # load rate per article
        self.load_rate_per_shelf = 60/60 # load rate per article onto shelf
        self.facing_rate = 40 # a time to face the aisle
        self.cleaning_time = 60 # a time to clean the back dock
        self.data = df
        self.d_pallet =  self.count_articles_per_pallet()
        self.store_aisles = num_aisles
        self.total_time_nightfill_required = self.total_time_required()

    def count_articles_per_pallet(self):
        data = self.data
        d_pallet = {}
        dff = self.data.groupby(['pallet'])['tot_qty'].sum()
        d_pallet = dff.to_dict()               
        return d_pallet

    def split_load(self):

        total_amt_time = 0        
        total_sum = sum(self.d_pallet.values())
        total_amt_time = total_sum * self.load_rate_article        
        return total_amt_time


    def put_on_shelf(self):
        total_amt_time = 0    
        total_sum = sum(self.d_pallet.values())
        total_amt_time = total_sum * self.load_rate_per_shelf        
        return total_amt_time

    def face_shop(self):
        total_amt_time = 0        
        total_amt_time = self.store_aisles * self.facing_rate
        return  total_amt_time

    def cleaning_back_dock(self):
        total_amt_time = self.cleaning_time       
        return total_amt_time

    def total_time_required(self):
        return self.split_load() + self.put_on_shelf()+ self.face_shop()+ self.cleaning_back_dock()  #SARKIS RUN FILL THIS IN+self...>

    def run_code(self):
        ttr_min = self.total_time_required()
        ttr_hours = round(self.total_time_required()/60 ,2)
        print("The total time required to complete the night fill work is " ,ttr_min,"minute = " , ttr_hours, 'Hours')
        return ttr_min

    def total_articles(self):
        ta = self.data['tot_qty'].sum()
        return int(ta)

def todo(**context):
    df = pd.read_csv(path, index_col=0)         
    nf = NightFill(df, 13)
    tt = nf.run_code()
    return tt
   
def total_amount(**context):
    df = pd.read_csv(path, index_col=0)         
    nf = NightFill(df, 13)
    taa = nf.total_articles()#Total articles amount
    return taa


with models.DAG(
    dag_id="nightfill_test_extension",
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
 