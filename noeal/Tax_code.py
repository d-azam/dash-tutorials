import numpy as np
import pandas as pd
import os
class Tax:
    
    def __init__(self, df_file):
        self.name = 'Noeal'
        self.data = pd.read_csv(df_file)
        self.tax_rate = {0: 0, 18201: 0.19, 70000: 0.32 , 150000: 0.40}
        self.total_income = self.getTotalIncome()
        self.total_deductions = self.getTotalDeductions()
        self.net_income=self.CalculateNetIncome()
        self.tax_rate_to_apply = self.getTaxRate()
        self.Medicare_levy=self.Medicarelevy()
        self.Total_Tax=self.CalculateTax()
        self.columns_n=self.SeparateColumnsNetIncome()
        self.columns_d=self.SeparateColumnsTotalDeductions()
        self.columns_TI=self.SeparateColumnsTotal_Income()
        self.columns_TaxR=self.SeparateColumnsTax_Rate()
        self.Sum_Total_Income=self.SumTotalIncome()
    
    def getTotalIncome(self):
        # Gettt total Income - by adding all the income columns
        df = pd.DataFrame(self.data)
        df['Total_Income'] = df.apply(lambda row: row.salary_income + row.dividends_income + row.capital_gains_income + row.rent_income + row.franked_credits_income, axis = 1)
        self.data =df
        return(df)
    
    def getTotalDeductions(self):
        # Gett total Deductions - by adding all the deductions columns
        #self.data = new_df
        df = pd.DataFrame(self.data)
        df['Total_Deduction'] = df.apply(lambda row: row.travel_expenses + row.interest_on_investment_property_exenses + row.self_education_expenses + row.self_education_expenses_1, axis = 1)
        self.data =df
        return(df)
    
    def CalculateNetIncome(self):
        # Calculate Net Income = TotalIncome-TotalDeduction
         #self.getTotalIncome()
        #self.getTotalDeductions()
        df = pd.DataFrame(self.data)
        df['Net_Income']=df['Total_Income']-df['Total_Deduction']
        #self.data = new_df
        return(df)
    

    def getTaxRate(self):
        # Return the data with a new column with the appropriate tax rate for each user id
        #print(self.data.head())
        #self.data = new_df
        df = pd.DataFrame(self.data)
        #self.getTotalIncome()
        conditions = [
        (df['Net_Income'] <18201),
        (df['Net_Income'] >=18201) & (df['Net_Income'] <=45000),
        (df['Net_Income'] >45000) & (df['Net_Income'] <= 120000),
        (df['Net_Income'] > 120000) & (df['Net_Income'] <= 180000),
        (df['Net_Income'] > 180000)
        ]
        #values=[5,5,5,5,5]
        #values=[0,0.19,((5092+((0.32*df['Net_Income']-45000)/df['Net_Income']))/100),(29467+(df['Net_Income']-120000)*0.40)/df['Net_Income'],(51667+(df['Net_Income']-180000)*0.45)/df['Net_Income'] ]
        values=[0,(0+(df['Net_Income']-18201)*0.19)/df['Net_Income'],(5092+(df['Net_Income']-45000)*0.31)/df['Net_Income'],(29467+(df['Net_Income']-120000)*0.40)/df['Net_Income'],(51667+(df['Net_Income']-180000)*0.45)/df['Net_Income'] ]
        df['Tax_Rate'] = np.select(conditions, values) 
        return df
    
    def Medicarelevy(self):
        # Medicarelevy When Net Income > 180000 
        #self.data = new_df
        df = pd.DataFrame(self.data)
        #self.getTotalIncome()
        conditions = [
        (df['Net_Income'] > 180000)
        ]
        #values=[0,0.19,((5092+((0.32*df['Net_Income']-45000)/df['Net_Income']))/100),(29467+(df['Net_Income']-120000)*0.40)/df['Net_Income'],(51667+(df['Net_Income']-180000)*0.45)/df['Net_Income'] ]
        values=[df['Net_Income'] * 0.02 ]
        df['Medicarelevy'] = np.select(conditions, values) 
        return df

    def CalculateTax(self):
        # Calculate Total tax for each individual :( Net Income * TaxRate)
        df = pd.DataFrame(self.data)
        self.getTaxRate()
        df["Total_Tax"]=df['Net_Income'] *  df['Tax_Rate'] + df['Medicarelevy']
        #self.data = new_df
        return(df)
    
    def SumTotalIncome (self):
         #Select Columns
        df = pd.DataFrame(self.data)
        df['SumTotalIncome']=df['Total_Income'].sum()
        #self.data = new_df
        return(df)
    
    def SeparateColumnsNetIncome (self):
        # Select Columns
        df = pd.DataFrame(self.data)
        df2 = df[["Net_Income"]]
        #self.data = new_df
        return(df2)
    
    def SeparateColumnsTotalDeductions (self):
        # Select Columns
        df = pd.DataFrame(self.data)
        df2 = df[['Total_Deduction']]
        #self.data = new_df
        return(df2)
    
    def SeparateColumnsTotal_Income (self):
        # Select Columns
        df = pd.DataFrame(self.data)
        df2 = df[['Total_Income']]
        #self.data = new_df
        return(df2)
    
    def SeparateColumnsTax_Rate (self):
        # Select Columns
        df = pd.DataFrame(self.data)
        df2 = df[['Tax_Rate']]
        #self.data = new_df
        return(df2)
    
    def SeparateColumnsTax_Rate (self):
        # Select Columns
        df = pd.DataFrame(self.data)
        df2 = df[['Tax_Rate']]
        #self.data = new_df
        return(df2)
    
    
    
    #def SeparateColumnsTTotal_Tax (self):
        # Select Columns
        #self.CalculateTax()
        #df = pd.DataFrame(self.data)
        #df2 = df[["Total_Tax"]]
        #self.data = new_df
        #return(df2)    