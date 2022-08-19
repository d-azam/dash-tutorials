import numpy as np
import pandas as pd
import os
class Tax:
    
    def __init__(self, df_file):
        self.name = 'Noeal'
        self.data = pd.read_csv(df_file)
        self.tax_rate = {0: 0, 18201: 0.19, 70000: 0.32 , 150000: 0.40}
        self.tax_rate_to_apply = self.getTaxRate()
        self.total_income = self.getTotalIncome()
        self.total_deductions = self.getTotalDeductions()
    
    def getTotalIncome(self):
        # Gettt total Income - by adding all the income columns
        df = pd.DataFrame(self.data)
        df['Total_Income'] = df.apply(lambda row: row.salary_income + row.dividends_income + row.capital_gains_income + row.rent_income + row.franked_credits_income, axis = 1)
        #self.data = new_df
        return(df)
    
    def getTaxRate(self):
        # Return the data with a new column with the appropriate tax rate for each user id
        print(self.data.head())
        #self.data = new_df
        df = pd.DataFrame(self.data)
        self.getTotalIncome()
        conditions = [
        (df['Total_Income'] <18201),
        (df['Total_Income'] >=18201) & (df['Total_Income'] <=70000),
        (df['Total_Income'] >70000) & (df['Total_Income'] <= 150000),
        (df['Total_Income'] > 150000)
        ]
        values=[0,0.19, 0.32,0.40]
        df['Tax_Rate'] = np.select(conditions, values) 
        return df


    def getTotalDeductions(self):
        # noealGett total Deductions - by adding all the deductions columns
        #self.data = new_df
        df = pd.DataFrame(self.data)
        df['Total_Deduction'] = df.apply(lambda row: row.travel_expenses + row.interest_on_investment_property_exenses + row.self_education_expenses + row.self_education_expenses_1, axis = 1)
        return(df)
     

    def CalculateTax(self):
        # Calculate Total tax for each individual
        df = pd.DataFrame(self.data)
        self.getTaxRate()
        df["Total_Tax"]=df['Total_Income'] *  df['Tax_Rate']
        #self.data = new_df
        return(df)
        