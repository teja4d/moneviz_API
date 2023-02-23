import datetime
import pandas as pd;
import uuid;
import squarify;
import numpy as np;
import random;


class CategorizeData:
    def __init__(self, file_path):
        self.x = 0
        self.y = 0
        self.category = None
        self.file_path = file_path
        self.df = pd.read_csv(file_path, header=0)
        self.df = self.df.rename(columns={'Transaction Date': 'transaction_date', 'Transaction Type': 'transaction_type', 'Transaction Description': 'transaction_description', 'Debit Amount': 'debit_amount', 'Credit Amount': 'credit_amount', 'Balance': 'balance'})
        self.df.columns = self.df.columns.str.replace(' ', '_')
        self.df['transaction_description'] = self.df['transaction_description'].str.replace(' ', '_').str.lower()
        self.df.columns = self.df.columns.str.lower()
        self.df['number'] = range(1, len(self.df) + 1)
        #add column date_number to df and assign number with same date
       # self.df['date_number'] = self.df.groupby(['transaction_date']).cumcount() + 1
        #add 0 if the value is empty in credit_amount and debit_amount
        self.df['credit_amount'] = self.df['credit_amount'].fillna(0)
        self.df['debit_amount'] = self.df['debit_amount'].fillna(0)
        self.df['transaction_type'] = self.df['transaction_type'].fillna('BI')
        #add column name transaction_id in the beginning of the dataframe
        self.df.insert(0, 'transaction_id', self.df.index)
        self.df['transaction_id'] = self.df.apply(lambda x: uuid.uuid4().hex, axis=1)
        self.df['type'] = self.df.apply(lambda x: 'credit' if x['credit_amount'] > 0 and x['debit_amount'] == 0 else 'debit', axis=1)
        self.df = CategorizeData.categorize_dataBytype(self, self.df)
        #store as cache for speed
    def categorize_dataBytype(self,df):
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%d/%m/%Y')
        df['day_of_week'] = df['transaction_date'].dt.day_name()
        df['category_spend'] = np.where(df['transaction_type'].isin(['BP','DD','SO','PAY',]), 'bill_payments',
        np.where(df['transaction_type'].isin(['CPT']), 'cash_point'
        ,np.where(df['transaction_type'].isin(['FEE']), 'account_fees',np.where(df['transaction_type'].isin(['TFR','FPO']),'transfer',np.where(df['transaction_type'].isin(['CHQ']), 'cheque_payments', np.where(df['transaction_type'].isin(['DEP']), 'deposits', np.where(df['transaction_type'].isin(['FPI','BGC','BI']), 'income', np.where(df['transaction_type'].isin(['DEB']), 'Shopping', 'other'))))))))
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('tesco|lidl|asda|aldi|sains|shop|food|sports|poundland|retail|mark|store|mart|spar|co-op') & df['debit_amount'] != 0, 'instore_purchase_debit', 'others')
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & ((df['transaction_description'].str.lower().str.contains('amazon|online|audible|ink_uk|amzn|amznmktplace|online|www.|.co.uk|.com') & (df['debit_amount'] != 0) & (df['credit_amount'] == 0))|
        (df['transaction_description'].str.lower().str.startswith('ama')) & (df['debit_amount'] != 0) & (df['credit_amount'] == 0)),'online_shopping_debit', df['sub_category']);
        #sub category if starts with 'ama' or amz
        #df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & (df['transaction_description'].str.lower().str.startswith('ama')) & (df['debit_amount'] != 0) & (df['credit_amount'] == 0), 'online_shoping_debit', df['sub_category']);
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & (df['transaction_description'].str.lower().str.startswith('ama')) & (df['debit_amount'] == 0) & (df['credit_amount'] != 0), 'online_shoping_refund', df['sub_category']);
        #df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.startswith('amz'), 'online_shoping_debit', df['sub_category']);
        df['sub_category'] = np.where(df['sub_category'].isin(['cash_point']) & (df['debit_amount'] != 0) & (df['credit_amount'] == 0)  , 'cash_point_withdrawals', df['sub_category'])
        df['sub_category'] = np.where(df['sub_category'].isin(['cash_point']) & (df['debit_amount'] == 0) & (df['credit_amount'] != 0)  , 'cash_point_deposits', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DD']) & df['transaction_description'].str.lower().str.contains('gym|internet|phone|rent|utilities|counc|water'), 'bill_payments', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('gym|internet|phone|rent|utilities|counc|water'), 'bill_payments', df['sub_category'])
        #sub category cash and based on transaction_description with save
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('save'), 'savings', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('save'), 'savings', df['sub_category'])
        #df['sub_category'] = np.where(df['transaction_type'].isin(['FPO']) & df['debit_amount'] != 0, 'money_transfer_debit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['TFR']) & df['debit_amount'] != 0, 'money_transfer_debit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['TFR']) & df['credit_amount'] != 0, 'money_transfer_credit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEP','PAY']) &  df['transaction_description'].str.lower().str.contains('club_lloyds')& (df['debit_amount'] == 0) & (df['credit_amount'] != 0) , 'bank_fee_credit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEP','PAY'])  & df['transaction_description'].str.lower().str.contains('club_lloyds') & (df['debit_amount'] != 0) & (df['credit_amount'] == 0), 'bank_fee_debit', df['sub_category'])
        #sub category based on transaction_description with dd and only debit and transaction_description with club lloyds regex expression
        df['sub_category'] = np.where(df['transaction_type'].isin(['FEE','DEB']) & df['transaction_description'].str.lower().str.contains('fee') & df['debit_amount'] != 0, 'bank_fee_debit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('share|trading212uk')  & df['debit_amount'] != 0, 'investment', df['sub_category'])
        #sub category based on transaction_description with dd and only debit and transaction_description with names
        df['sub_category'] = np.where(df['transaction_type'].isin(['FPI']) & df['credit_amount'] != 0 & df['transaction_description'].str.lower().str.contains(''), 'money_transfer_credit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['BGC']) & df['credit_amount'] != 0 & df['transaction_description'].str.lower().str.contains('university|univ') & (df['debit_amount'] != 0) & (df['credit_amount'] == 0), 'income', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['BI']) & df['credit_amount'] != 0 & df['transaction_description'].str.lower().str.contains('interest'), 'interest', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['SO']) & df['credit_amount'] != 0, 'income', df['sub_category'])
        #not contains
        df['sub_category'] = np.where(df['transaction_type'].isin(['SO']) & df['debit_amount'] != 0 & df['transaction_description'].str.lower().str.contains('save',na=False), 'savings', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['BP']) & df['debit_amount'] != 0 & df['transaction_description'].str.lower().str.contains('save',na=False), 'savings', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['SO']) & df['debit_amount'] != 0 & df['transaction_description'].str.lower().str.contains('save'), 'savings', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('non-')
        & df['debit_amount'] != 0, 'bank_fee_debit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('easyjet|moving|travel|hotel|airport|ticket|booking|holiday|rooms|airbnb|stay|taxi|trip|uber|tickets|rail|train') & (df['debit_amount'] != 0) & (df['credit_amount'] == 0), 'travel', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DEB']) & df['transaction_description'].str.lower().str.contains('easyjet|moving|travel|hotel|airport|ticket|booking|holiday|rooms|airbnb|stay|taxi|trip|uber|tickets|rail|train') & (df['debit_amount'] == 0) & (df['credit_amount'] != 0), 'travel_refund', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['DD']) & df['debit_amount'] != 0 , 'bill_payments', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['CHQ']) & df['credit_amount'] != 0 , 'deposits_credit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_type'].isin(['CHQ']) & df['debit_amount'] != 0 , 'deposits_debit', df['sub_category'])
        df['sub_category'] = np.where(df['transaction_description'].str.lower().str.contains('bakery|kfc|starbucks|vegan|doughnotts|greggs|deliveroo|mcdo|coffee|cafe|caffe|atrium|restaurant|subway|desserts|pizza|dosa'), 'Food',df['sub_category'])
        #groupby transaction_descriptions where sub_category is others
        df['transaction_description'] = df['transaction_description'].str.lower()
        df['description_category'] = df.groupby('transaction_description')['sub_category'].transform(lambda x: x.value_counts().index[0])
        df['sub_category'] = np.where(df['sub_category'] == 'others', df.groupby('transaction_description')['sub_category'].transform('min'), df['sub_category'])
        #df with only sub_category others
        df['sub_category'] = np.where(df['sub_category'] == 'others', df['description_category'], df['sub_category'])
        df['description_category'] = np.where((df['type'] == 'debit') & (df['sub_category'].str.lower().str.contains('debit')),  df['sub_category'],df['description_category'])
        df['description_category'] = np.where((df['type'] == 'credit') & (df['sub_category'].str.lower().str.contains('credit|income')),  df['sub_category'],df['description_category'])
        df[df['sub_category'] != df['description_category']].sort_values(by=['description_category'], ascending=False)
        #if transaction_description and sub_category is 'others' then give a new name in sepearte column and transform max count not others
        df['description_category'] = np.where(df['sub_category'] == 'others', df['transaction_description'], df['description_category'])
        #remove description_category column
        df.drop(['description_category'], axis=1, inplace=True)
        df['transaction_date'] = df['transaction_date'].dt.strftime('%d/%m/%Y')
        #sort by transaction_date
        df.sort_values(by=['number'], ascending=False, inplace=True)
        return df

    def groupDatabyType(self, start_date = None, end_date = None):
        df = self.df
        df_group = df.groupby(['type']).agg({'credit_amount': 'sum', 'debit_amount': 'sum'})
        #add column label to df_group
        df_group['label'] = df_group.apply(lambda x: x.name, axis=1)
        #add type column to df_group
        df_group['type'] = df_group.apply(lambda x: x.name, axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['rects'] = self.squarifyData(df_group)
        df_group['x'] = df_group.apply(lambda x: x['rects']['x'], axis=1)
        df_group['y'] = df_group.apply(lambda x: x['rects']['y'], axis=1)
        df_group['width'] = df_group.apply(lambda x: x['rects']['dx'], axis=1)
        df_group['height'] = df_group.apply(lambda x: x['rects']['dy'], axis=1)
        #df_group['children_rects'] = df_group.apply(lambda x: self.groupDatabyTransactionType(df_group), axis=1)
        df_group['category'] = df_group.apply(lambda x: "transaction", axis=1)
        #df_group['line_graph'] = df_group.apply(lambda x: self.getLinePoints())
        df_group['recent_transaction'] = df_group.apply(lambda x: df[df['type'] == x['label']].tail(5), axis=1)
        df_group['rects_children'] = df_group.apply(lambda x: self.groupDatabyTransactionType(type = x['label'], x = x['x'], y=x['y'], width=x['width'], height=x['height'],child=True), axis=1)
        df_group.drop(['x', 'y', 'width', 'height'], axis=1, inplace=True)
        return df_group
        
    def groupDatabyTransactionType(self,type=None,df=None,x=None,y=None,width=None,height=None,child=False):
        if df is None:
             df = self.df
        #filter items with type == 'credit'
        if type:
              df_filter = df[df['type'] == type]
        df_group = df_filter.groupby(['transaction_type']).agg({'credit_amount': 'sum', 'debit_amount': 'sum'})     
        #add column label to df_group
        #add column type to df_group
        df_group['type'] = df_group.apply(lambda x: type, axis=1)
        df_group['label'] = df_group.apply(lambda x: x.name, axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['rects'] = self.squarifyData(df_group,x=x,y=y,width=width,height=height,child=child)
        if child == True:
            return df_group
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['category'] = df_group.apply(lambda x: "description", axis=1)
        df_group['x'] = df_group.apply(lambda x: x['rects']['x'], axis=1)
        df_group['y'] = df_group.apply(lambda x: x['rects']['y'], axis=1)
        df_group['width'] = df_group.apply(lambda x: x['rects']['dx'], axis=1)
        df_group['height'] = df_group.apply(lambda x: x['rects']['dy'], axis=1)
        df_group['rects_children'] = df_group.apply(lambda x: self.groupDatabyDescription(expense=type,type = x['label'], x = x['x'], y=x['y'], width=x['width'], height=x['height'],child=True), axis=1)
        df_group['recent_transaction'] = df_group.apply(lambda x: df_filter[df_filter['transaction_type'] == x['label']].tail(5), axis=1)
        df_group.drop(['x', 'y', 'width', 'height'], axis=1, inplace=True)
        return df_group
 
    def groupDatabyCategorySpend(self,expense=None,df=None,x=None,y=None,width=None,height=None):
        df = self.df
        #filter items with type == 'credit'
        if expense:
             df_filter = df[df['type'] == expense]
        else:
             df_filter = df
        #if type:
        #      df_filter = df[df['type'] == type]
        df_group = df_filter.groupby(['category_spend']).agg({'credit_amount': 'sum', 'debit_amount': 'sum'})        
        #add column label to df_group
        df_group['label'] = df_group.apply(lambda x: x.name, axis=1)
        df_group['type'] = df_group.apply(lambda x: 'credit' if x.credit_amount > 0 else 'debit', axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['rects'] = self.squarifyData(df_group)
        df_group['category'] = df_group.apply(lambda x: "subcategory", axis=1)
        #df_group['recent_transaction'] = df_group.apply(lambda x: df_filter[df_group['type'] == x['label']].tail(5), axis=1)
        return df_group

    def groupDatabySubCategory(self,height=None, width=None, x=None, y=None,type=None):
        df = self.df
        #filter items with type == 'credit'
        df_filter = df
        df_group = df_filter.groupby(['sub_category']).agg({'credit_amount': 'sum', 'debit_amount': 'sum'})
        #df_group['type'] = df_group.apply(lambda x: 'credit' if x.credit_amount > 0 else 'debit', axis=1)     
        #add column label to df_group
        df_group['label'] = df_group.apply(lambda x: x.name, axis=1)
        df_group['label'] = df_group.apply(lambda x: x['label'].replace('_',' '), axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['rects'] = self.squarifyData(df_group)
        df_group['category'] = df_group.apply(lambda x: "description", axis=1)
        #df_group['recent_transaction'] = df_group.apply(lambda x: df_filter[df_group['type'] == x['label']].tail(5), axis=1)
        return df_group

    def groupDatabyDescription(self,type=None,expense=None,df=None,x=None,y=None,width=None,height=None,child=False):
        #group by description and transaction type
        if df is None:
             df = self.df
        if expense:
            df_filter = df[df['type'] == expense]
        else:
            df_filter = df
        if type:
            df_filter = df_filter[df_filter['transaction_type'] == type]
        #keep transaction_date after grouping
        df_group = df_filter.groupby(['transaction_description'],as_index=False).agg({'credit_amount': 'sum', 'debit_amount': 'sum', 'transaction_date': 'first'})
        #add column type to df_group
        df_group['type'] = df_group.apply(lambda x: 'credit' if x['credit_amount'] > 0 else 'debit', axis=1)
        #add column label to df_group
        df_group['label'] = df_group.apply(lambda x: x['transaction_description'], axis=1)
        #add credit or debit type
        #df_group['type'] = df_group.apply(lambda x: 'credit' if x['credit_amount'] > 0 else 'debit', axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['rects'] = self.squarifyData(df_group,x=x,y=y,width=width,height=height,child=child)
        if child == True:
            return df_group
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['category'] = df_group.apply(lambda x: "date", axis=1)
        df_group['x'] = df_group.apply(lambda x: x['rects']['x'], axis=1)
        df_group['y'] = df_group.apply(lambda x: x['rects']['y'], axis=1)
        df_group['width'] = df_group.apply(lambda x: x['rects']['dx'], axis=1)
        df_group['height'] = df_group.apply(lambda x: x['rects']['dy'], axis=1)
        #print(x,y,width,height)
        df_group['rects_children'] = df_group.apply(lambda x: self.groupDatabyDate(df=df_filter,expense=x['type'],type = x['label'], x = x['x'], y=x['y'], width=x['width'], height=x['height'],child=True), axis=1)
        #df_group['rects_children'] = df_group.apply(lambda x: self.groupDatabyDate(df=df_group,expense=expense,type = x['label'],x = x['x'], y=x['y'], width=x['width'], height=x['height'],child=True), axis=1)
        df_group['recent_transaction'] = df_group.apply(lambda x: df_filter[df_filter['transaction_description'] == x['label']].tail(5), axis=1)
        df_group.drop(['x', 'y', 'width', 'height'], axis=1, inplace=True)
        
        #print max(df_group['total'])
        return df_group

    def groupDatabyDate(self,df=None,type=None,expense=None,x=None,y=None,width=None,height=None,child=False):
        if df is None:
             df =  self.df
        #filter df with transaction_type type = 'DEB'
        #print column names
        if expense:
            df_filter = df[df['type'] == expense]
        else:
            df_filter = df
        if type:
            df_filter = df_filter[df_filter['transaction_description'] == type]
        df_recent = df_filter
        df_group = df_filter.groupby(['transaction_date'],as_index=False).agg({'credit_amount': 'sum', 'debit_amount': 'sum'}) 
        df_group['type'] = df_group.apply(lambda x: 'credit' if x['credit_amount'] > 0 else 'debit', axis=1)
        #add column label to df_group
        df_group['label'] = df_group.apply(lambda x: x['transaction_date'], axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['rects'] = self.squarifyData(df_group,x=x,y=y,width=width,height=height,child = child)
        if child == True:
            return df_group
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['category'] = df_group.apply(lambda x: "day", axis=1)
        df_group['x'] = df_group.apply(lambda x: x['rects']['x'], axis=1)
        df_group['y'] = df_group.apply(lambda x: x['rects']['y'], axis=1)
        df_group['width'] = df_group.apply(lambda x: x['rects']['dx'], axis=1)
        df_group['height'] = df_group.apply(lambda x: x['rects']['dy'], axis=1)
        
        df_group['rects_children'] = df_group.apply(lambda x: self.groupDatabyDay(df=df_filter,expense=type,type = x['label'], x = x['x'], y=x['y'], width=x['width'], height=x['height'],child=True), axis=1)
        df_group['recent_transaction'] = df_group.apply(lambda x: df_recent[df_recent['transaction_date'] == x['label']].tail(5), axis=1)
        df_group.drop(['x', 'y', 'width', 'height'], axis=1, inplace=True)
        
        return df_group
    def groupDatabyDay(self,df=None,type=None,expense=None,child=False,x=None,y=None,width=None,height=None):
        if df is None:
            df = self.df
        if expense:
            df_filter = df[df['type'] == expense]
        else:
            df_filter = df
        if type:
            df_filter = df[df['transaction_date'] == type]
        df_group = df_filter     
        #add column label to df_group
        df_group['label'] = df_group.apply(lambda x: x['transaction_date'], axis=1)
        #add column type to df_group
        df_group['type'] = df_group.apply(lambda x: 'credit' if x['credit_amount'] > 0 else 'debit', axis=1)
        #add column total to df_group
        df_group['total'] = df_group.apply(lambda x: x['credit_amount'] + x['debit_amount'], axis=1)
        df_group['category'] = df_group.apply(lambda x: "day_transactions", axis=1)
        df_group['percentage'] = df_group.apply(lambda x: (x['total']/df_group['total'].sum())*100, axis=1)
        df_group['rects'] = self.squarifyData(df_group,x=x,y=y,width=width,height=height,child = child)
        return df_group
    def hslColor(self,percentage ,h,l,s):
        if percentage > 50:
            l = l - (percentage/100)
            h = h + (percentage/100)

        else:
            l = l + (percentage/100)
            h = h - (percentage/100)
        return "hsl(" + str(h) + "," + str(s) + "%," + str(l) + "%)"

    def squarifyData(self,df_group,width=1200,height=600,x=None,y=None,child=False):
        #sort df_group by total
        if width == None:
            width = 1200
        if height == None:
            height = 600
        if x == None:
            x = 0
        if y == None:
            y = 0
        df_group = df_group.sort_values(by='total', ascending=False)
        values = df_group['total'].tolist()
        values.sort(reverse=True)
        values = squarify.normalize_sizes(values, width, height)
        if child==True:
            rects = squarify.squarify(values, x, y, width, height)
        else:
            rects = squarify.padded_squarify(values, x, y, width, height)
        #sumofTotal = sum(values)
        #add color to rects based on area
        #generate random number between 0 and 360
        # h = random.randint(200,360)
        # l = random.randint(20,80)
        # s = random.randint(20,80)
        # for i in range(len(rects)):
        #     rects[i]['color'] = self.hslColor(values[i]/sumofTotal,h,l,s)
        #rects as child to df_group
        df_group['rects'] = rects
        #add rects to df_group
        df_group['rects'] = df_group.apply(lambda x: x['rects'], axis=1)
        return df_group['rects']

    def getLinePoints(self,width=None,height=None,):
        #python mdoule to generate line graph points over time
        if width == None:
            width = 1200
        if height == None:
            height = 600
        #get data from df
        df = self.df
        #get avg balace each year
        #convert transaction+date to date time
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df_year = df.groupby(df['transaction_date'].dt.year).mean()
        #generate x,y coordinates over balance and year
        x = df_year.index.tolist()
        y = df_year['credit_amount'].tolist()
        #generate line graph points
        #return x,y as df
        df_line = pd.DataFrame({'x':x,'y':y})
        #If using all scalar values, you must pass an index
        df_line['category'] = 'line'
        df_line['rects'] = df_line.apply(lambda x: {'x':x['x'],'y':x['y'],'dx':width/len(x),'dy':height/len(y)}, axis=1)
        #print(df_line)
        

        

