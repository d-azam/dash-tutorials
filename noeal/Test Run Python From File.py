import datetime
#import pendulum
import numpy as np
import pandas as pd
import os
import json
import csv

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
        #return self.data.loc[:'Total_Income'].to_csv("gs://us-central1-rtla-dev-v2-bacbcff2-bucket//data//ti.csv",header=False ,index=False,quoting=1)
        #n=self.data.loc[:'Total_Income'].to_json()
        n=self.data.loc[:'Total_Income'].to_csv('/home/airflow/gcs/data/ti.csv',index=False)
        #m=pd.read_json(n)
        #return self.data.loc[:'Total_Income'].to_csv("gs://us-central1-rtla-dev-v2-bacbcff2-bucket/data/ti.csv")
        #m= pd.DataFrame(json.loads(n))
        #return m.to_csv("gs://us-central1-rtla-dev-v2-bacbcff2-bucket/data/ti.csv", sep=',' ,escapechar='\\', quoting=csv.QUOTE_ALL, encoding='utf-8' )
        return n
        
def Total_incomee(**context):
    taxx = Tax(path)
    taxx_ratee=taxx.getTotalIncome()
    return taxx_ratee