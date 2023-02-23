import sys
import numpy as np
import pandas as pd
import csv as csv
import uuid

#export function

class FilterData:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = pd.read_csv(file_path, header=0)
    def filter_data(self, start_date = None, end_date = None , moving_average = None):
        df = self.df
        #remove spaces from column names
        df.columns = df.columns.str.replace(' ', '_')
        #rename column names to lower case
        df.columns = df.columns.str.lower()
        #set values to 0 for credit and debit if null or blank
        df['credit_amount'] = df['credit_amount'].fillna(0)
        df['debit_amount'] = df['debit_amount'].fillna(0)
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%d/%m/%Y')
        #group by transaction_date and average with precesion 2 the balance add title as average balance average credit and average debit       
        df = df.groupby('transaction_date').agg({'balance': ['first','last'],'credit_amount':['sum','last'],'debit_amount':['sum','last']}).round(2).reset_index()
        #rename columns for credit and debit
        df.columns = ['transaction_date','closing_balance','opening_balance','credit_amount','creditamount_first','debit_amount','debitamount_first']
        df['opening_balance'] = df['opening_balance'] - df['creditamount_first'] + df['debitamount_first']
        #remove columns
        df = df.drop(['creditamount_first','debitamount_first'], axis=1)
        #add column name as transaction_id
        df['transaction_id'] = df.index
        #add values to transaction_id column
        df['transaction_id'] = df['transaction_id'].apply(lambda x: uuid.uuid4().hex)
        #add a new column with the day of the week
        df['day_of_week'] = df['transaction_date'].dt.day_name()
        #average balance in specif date range
        if start_date and end_date:
            df = df[(df['transaction_date'] >= start_date) & (df['transaction_date'] <= end_date)]
        #moving average for last 90 days
        #df['average_balance'] = df['closing_balance'].rolling(window=35,center=True).mean()
        #add 0 if null
        #df['average_balance'] = df['average_balance'].fillna(0)
        #else:
            #df['average_balance'] = 0
        return df
    
    def filter_data_asJson(self, start_date = None, end_date = None ):
        #convert start_date and end_date iso format to datetime
        start_date = pd.to_datetime(start_date, format='%Y-%m-%d',errors='coerce')
        end_date = pd.to_datetime(end_date, format='%Y-%m-%d',errors='coerce')
        df = self.filter_data(start_date, end_date,moving_average = None)
        #data from start date to end date
        #if start_date and end_date:
            #df = df[(df['transaction_date'] >= start_date) & (df['transaction_date'] <= end_date)]
        #convert date to dd/mm/yyyy without time
        df['transaction_date'] = df['transaction_date'].dt.strftime('%d/%m/%Y')
        #replcae _ with space for transaction_description
        #df['transaction_description'] = df['transaction_description'].str.replace('_', ' ')
        df = df.to_json(orient='records')
        return df
    #function to send max average balance, min average balance, average balance , average credit, average debit
    def data_summary(self, start_date = None, end_date = None ):
        df = self.filter_data(start_date=start_date, end_date=end_date)
        #from float to int and round
        max_balance = df['closing_balance'].max()
        min_balance = df['closing_balance'].min()
        average_balance = df['closing_balance'].mean()
        average_credit = df['credit_amount'].mean()
        average_debit = df['debit_amount'].mean()
       #send date in the format dd/mm/yyyy without time
        #max_average_balance_date = df[df['average_balance'] == max_balance]['transaction_date'].dt.strftime('%d/%m/%Y').iloc[0]
        #min_average_balance_date = df[df['average_balance'] == min_balance]['transaction_date'].dt.strftime('%d/%m/%Y').iloc[0]
        #send as api response
        return {'max_average_balance': max_balance, 'min_average_balance': min_balance, 'average_balance': average_balance, 'average_credit': average_credit, 'average_debit': average_debit,}
  #function to get moving average for last 90 days
    def moving_average(self, start_date = None, end_date = None , moving_average = None):
        if start_date == 'null' and end_date == 'null':
            start_date = None
            end_date = None
        start_date = pd.to_datetime(start_date, format='%Y-%m-%d',errors='coerce')
        end_date = pd.to_datetime(end_date, format='%Y-%m-%d',errors='coerce')
        df = self.filter_data(start_date=start_date, end_date=end_date)
       #moving average from last 90 days and future 3 days
       #sen all columns
        df['average_balance'] = df['closing_balance'].rolling(window=int(moving_average)).mean()
        #df['average_balance'] = df['closing_balance'].rolling(window=moving_average,center=True).mean()
        #add 0 if null
        df['average_balance'] = df['average_balance'].fillna(0)
        #convert date to dd/mm/yyyy without time
        df['transaction_date'] = df['transaction_date'].dt.strftime('%d/%m/%Y')
        df = df.to_json(orient='records')
        return df