import pandas as pd;
import re
import uuid

class DataTimeLine():
    def __init__(self,file_path,type):
        self.file_path = file_path
        self.df = pd.read_csv(file_path, header=0)
        self.df.columns = self.df.columns.str.replace(' ', '_')
        self.df.columns = self.df.columns.str.lower()
        self.df['id'] = self.df.groupby('transaction_description').ngroup() + 1
        self.df['type'] = self.df.apply(lambda x: 'credit' if x['credit_amount'] > 0  else 'debit', axis=1)
        self.df['transaction_date'] = pd.to_datetime(self.df['transaction_date'], format='%d/%m/%Y')
        #add     column sum of credit and debit
        self.df = self.df.fillna(0)
        self.df['amount'] = self.df['credit_amount'] + self.df['debit_amount']
        #get date from transaction_date i,e if transaction_date is 1/1/2020 then get 1
        self.df['date'] = self.df['transaction_date'].dt.day
        #get month from transaction_date i,e if transaction_date is 1/1/2020 then get 1
        self.df['month'] = self.df['transaction_date'].dt.month
        self.df['year'] = self.df['transaction_date'].dt.year
        self.df['first_transaction_date'] = self.df.groupby('id')['transaction_date'].transform('min')
        #last_transaction_date
        self.df['last_transaction_date'] = self.df.groupby('id')['transaction_date'].transform('max')
        self.df['last_transaction_date'] = pd.to_datetime(self.df['last_transaction_date'], format='%d/%m/%Y')
        self.df['first_transaction_date'] = pd.to_datetime(self.df['first_transaction_date'], format='%d/%m/%Y')
        self.df = self.df[self.df['type'] == type]
        #drop
        self.df.drop(['sort_code','account_number'],axis=1,inplace=True)

    def is_monthly_payment(self,description,type):
        data_frame = self.df
        t_first = data_frame[data_frame['id'] == description]['first_transaction_date'].iloc[0]
        t_last = data_frame[data_frame['id'] == description]['last_transaction_date'].iloc[0]
        #get months between first and last_transaction_date
        months = (t_last.year - t_first.year) * 12 + (t_last.month - t_first.month)
        # Load CSV file into pandas DataFrame
        #data_frame = pd.read_csv(file_path)
        # Extract potential monthly payments based on transaction_description
        data_frame['transaction_date'] = pd.to_datetime(data_frame['transaction_date'], format='%d/%m/%Y')
   #     Check if there are at least 3 potential weekly payments within the past 6 months
        # assume today is 1/1/2020
        transaction_date = pd.to_datetime(t_first,format='%d/%m/%Y');
        mask = data_frame['id'] == description
        potential_monthly_payments = data_frame[mask]
        six_months_ago = transaction_date - pd.DateOffset(months=months)
        num_potential_monthly_payments = potential_monthly_payments[potential_monthly_payments['transaction_date'] >= six_months_ago].shape[0]
        
        if num_potential_monthly_payments != 1 and num_potential_monthly_payments >= months-4 if months > 6  else months :
            return True
        else:
            return False
        
    def get_monthly_payment(self,type=None):
        df=self.df
        df['payment_cycle'] = df['id'].apply(lambda x: 'monthly' if self.is_monthly_payment(x,type) else 'irregular')
        df['transaction_date'] = df['transaction_date'].dt.strftime('%d/%m/%Y')
        df['first_transaction_date'] = df['first_transaction_date'].dt.strftime('%d/%m/%Y')
        df['last_transaction_date'] = df['last_transaction_date'].dt.strftime('%d/%m/%Y')
        #transaction_descrtion to lower case
        df['transaction_description'] = df['transaction_description'].str.replace(' ', '_').str.lower()
        #group by year
        return df
