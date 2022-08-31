import pandas as pd

path ="/home/airflow/gcs/data/night_fill.csv"

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