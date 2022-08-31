import datetime
import pendulum
import numpy as np
import pandas as pd
import os
import json
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from airflow.contrib.operators import bigquery_to_gcs
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator




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


path="/home/airflow/gcs/data/tax_dummy_data.csv"

   
class Tax:
    
    def __init__(self, df_file):
        self.name = 'Noeal'
        self.data =pd.read_csv(df_file)
        #self.tax_rate = {0: 0, 18201: 0.19, 70000: 0.32 , 150000: 0.40}
        self.total_income = self.getTotalIncome()
        #self.total_deductions = self.getTotalDeductions()
        #self.net_income=self.CalculateNetIncome()
        #self.tax_rate_to_apply = self.getTaxRate()
        #self.Medicare_levy=self.Medicarelevy()
        #self.Total_Tax=self.CalculateTax()
        #self.columns_n=self.SeparateColumnsNetIncome()
        #self.columns_d=self.SeparateColumnsTotalDeductions()
        #self.columns_TI=self.SeparateColumnsTotal_Income()
        #self.columns_TaxR=self.SeparateColumnsTax_Rate()
        #self.Sum_Total_Income=self.SumTotalIncome()
        

    def getTotalIncome(self):
        # Gettt total Income - by adding all the income columns
        #df = pd.DataFrame(self.data)
        self.data['Total_Income'] = self.data.apply(lambda row: row.salary_income + row.dividends_income + row.capital_gains_income + row.rent_income + row.franked_credits_income, axis = 1)
        #self.data =df
        #self.data[['Total_Income']]
        #print(self.data[['Total_Income']])
        #return self.data[['Total_Income']]
        #self.data['salary_income']
        ##print(self.data.loc[:'salary_income']) #[['salary_income']])
        #return self.data.loc[:'salary_income']
        #self.data[['Total_Income']]
        print(self.data.loc[:'Total_Income'])
        return self.data.loc[:'Total_Income'].to_json()
        
    
    #def getTotalDeductions(self):
        # Gett total Deductions - by adding all the deductions columns
        #self.data = new_df
        #df = pd.DataFrame(self.data)
        #self.data['Total_Deduction'] = self.data.apply(lambda row: row.travel_expenses + row.interest_on_investment_property_exenses + row.self_education_expenses + row.self_education_expenses_1, axis = 1)
        #self.data =df
        #return self.data['Total_Deduction']
    
    #def CalculateNetIncome(self):
        # Calculate Net Income = TotalIncome-TotalDeduction
         #self.getTotalIncome()
        #self.getTotalDeductions()
        #df = pd.DataFrame(self.data)
        #self.data['Net_Income']=self.data['Total_Income']- self.data['Total_Deduction']
        #self.data = new_df
        #return self.data['Net_Income']
    

    #def getTaxRate(self):
        # Return the data with a new column with the appropriate tax rate for each user id
        #print(self.data.head())
        #self.data = new_df
        #df = pd.DataFrame(self.data)
        #self.getTotalIncome()
        #conditions = [
        #(self.data['Net_Income'] <18201),
        #(self.data['Net_Income'] >=18201) & (self.data['Net_Income'] <=45000),
        #(self.data['Net_Income'] >45000) & (self.data['Net_Income'] <= 120000),
        #(self.data['Net_Income'] > 120000) & (self.data['Net_Income'] <= 180000),
        #(self.data['Net_Income'] > 180000)
        #]
        #values=[5,5,5,5,5]
        #values=[0,0.19,((5092+((0.32*df['Net_Income']-45000)/df['Net_Income']))/100),(29467+(df['Net_Income']-120000)*0.40)/df['Net_Income'],(51667+(df['Net_Income']-180000)*0.45)/df['Net_Income'] ]
        #values=[0,(0+(self.data['Net_Income']-18201)*0.19)/self.data['Net_Income'],(5092+(self.data['Net_Income']-45000)*0.31)/self.data['Net_Income'],(29467+(self.data['Net_Income']-120000)*0.40)/self.data['Net_Income'],(51667+(self.data['Net_Income']-180000)*0.45)/self.data['Net_Income'] ]
        #self.data['Tax_Rate'] = np.select(conditions, values) 
        #return self.data['Tax_Rate']
    
    #def Medicarelevy(self):
        # Medicarelevy When Net Income > 180000 
        #self.data = new_df
        #df = pd.DataFrame(self.data)
        #self.getTotalIncome()
        #conditions = [
        #self.data['Net_Income'] > 180000)
        #]
        #values=[0,0.19,((5092+((0.32*df['Net_Income']-45000)/df['Net_Income']))/100),(29467+(df['Net_Income']-120000)*0.40)/df['Net_Income'],(51667+(df['Net_Income']-180000)*0.45)/df['Net_Income'] ]
        #values=[self.data['Net_Income'] * 0.02 ]
        #self.data['Medicarelevy'] = np.select(conditions, values) 
        #return self.data['Medicarelevy']

    #def CalculateTax(self):
        # Calculate Total tax for each individual :( Net Income * TaxRate)
        #df = pd.DataFrame(self.data)
        #self.getTaxRate()
        #self.data["Total_Tax"]=self.data['Net_Income'] *  self.data['Tax_Rate']
        #self.data = new_df
        #return self.data["Total_Tax"]
    
       


def Total_incomee(**context):
    #dfr=Tax(path)
    #dfr = pd.read_csv(path,index_col=0)
    #df = pd.DataFrame(path,data)
    #salary_income
    taxx = Tax(path)
    taxx_ratee=taxx.getTotalIncome()
    return taxx_ratee
   
   

with models.DAG(
    dag_id="Noeal_Tax_Code",
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
    
    download_file = LocalFilesystemToGCSOperator(
    task_id="download_file",
    object_name='test_n.csv',
    bucket='bucket-test',
    filename='/home/airflow/gcs/data/test.csv',
    
    )



 
    #start = DummyOperator(task_id="start")

     #------------------------------------------------------------------------------------------------------------------
     #landing start
     #------------------------------------------------------------------------------------------------------------------

email = EmailOperator(
        task_id='send_email',
        to='nalbhnonesan@woolworths.com.au',
        subject='Airflow Alert',
        html_content="Total_incomeeeee is <b>{{ task_instance.xcom_pull(task_ids='Total_incomee') }} </b> ",
        provide_context = True,
        dag=dag
       
    )       
    
 
    
Total_incomee>>download_file>>email