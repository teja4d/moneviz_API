import os
import uuid
from flask_restful import Api
from flask import Flask, abort, jsonify, request
from flask_cors import CORS
from averagedata import FilterData
from dataCategory import CategorizeData
from timeline import DataTimeLine
import json
app = Flask(__name__)
CORS(app)
api = Api(app)
#define variable 
@app.route('/upload/csv', methods=['POST'])
def upload_csv():
    if request.method == 'POST':
        #get file from request
        f = request.files['file']
        save_path = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        #delete all files in data folder
        #give admin permission to delete files
        os.system('sudo chmod 9010 data')
        #delete all files in data folder
        os.system('sudo rm -rf data/*')
        for file in os.listdir(save_path):
            os.remove(os.path.join(save_path, file))
        #save file in data folder
        file_path = os.path.join(save_path,'data.csv')
        f.save(file_path)
        data = FilterData(file_path).filter_data_asJson(start_date=None,end_date=None)
        #datasummary = FilterData(file_path).data_summary()
        #send as api response in json format
        #response in date time format
        #send data in json format
        return data
        #return data
#get start date and end date from user in the request       
@app.route('/getsummary/date', methods=['GET'])
def get_summary():
    if request.method == 'GET':
        #find file in data folder
        #start_date,end date from url
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        # save_path = os.path.join(os.getcwd(), 'data')
        # file_path = os.path.join(file_path, 'data.csv')
        datasummary = FilterData(file_path).data_summary(start_date,end_date)
        #send as api response in json format
        return datasummary
#Custom date range
@app.route('/getsummary/date', methods=['POST'])
def get_transactions_by_date():
    if request.method == 'POST':
        #file_path = os.path.join(os.getcwd(), 'data', 'data3.csv')
        save_path = os.path.join(os.getcwd(), 'data')
        file_path = os.path.join(save_path, 'data.csv')
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        data = FilterData(file_path).filter_data_asJson(start_date, end_date)
        #send as api response in json format
        return data
#moving average
@app.route('/getsummary/movingaverage', methods=['GET'])
def get_moving_average():
    if request.method == 'GET':
        #find file in data folder
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        #save_path = os.path.join(os.getcwd(), 'data')
        #file_path = os.path.join(file_path, 'data.csv')
        #start_date,end date from url
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        #moving average as int
        moving_average = int(request.args.get('moving_average'))
        #moving average
        moving_average = request.args.get('moving_average')
        datasummary = FilterData(file_path).moving_average(start_date,end_date, moving_average)
        #send as api response in json format
        return datasummary

#data category
@app.route('/getsummary/category', methods=['GET'])
def get_category():
    if request.method == 'GET':
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        datasummary = CategorizeData(file_path).categorize_dataBytype()
        return datasummary
@app.route('/getsummary/category/summary', methods=['GET'])
def get_category_summary():
    if request.method == 'GET':
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        data_child = CategorizeData(file_path).categorizeData()
        return data_child

@app.route('/getsummary/category/summary/date', methods=['GET'])
def get_category_summary_date():
    if request.method == 'GET':
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        data_type = request.args.get('parent_type')
        child_type = request.args.get('child_type')
        expense_type = request.args.get('expense_type')
        
        if data_type == 'expense':
            data_child = CategorizeData(file_path).groupDatabyType()
        elif data_type == 'transaction':
            data_child = CategorizeData(file_path).groupDatabyTransactionType(type=child_type)
        elif data_type == 'category':
            data_child = CategorizeData(file_path).groupDatabyCategorySpend(expense=expense_type,type=child_type)
        elif data_type == 'sub_category':
            data_child = CategorizeData(file_path).groupDatabySubCategory(expense=expense_type,type=child_type)      
        elif data_type == 'description':
            data_child = CategorizeData(file_path).groupDatabyDescription(expense=expense_type,type=child_type)
        elif data_type == 'date':
            data_child = CategorizeData(file_path).groupDatabyDate(expense=expense_type,type=child_type)
        elif data_type == 'day':
            data_child = CategorizeData(file_path).groupDatabyDay(expense=expense_type,type=child_type)
        #local variable 'data_child' referenced before assignment
        else :
            abort(404)
        return data_child.to_json(orient='records')
@app.route('/getsummary/category/transaction', methods=['GET'])
def get_category_transaction():
    if request.method == 'GET':
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        data_type = request.args.get('parent_type')
        child_type = request.args.get('child_type')

        if data_type == 'category':
            data_child = CategorizeData(file_path).groupDatabyCategorySpend()
        elif data_type == 'sub_category':
            data_child = CategorizeData(file_path).groupDatabySubCategory()
        else :
            abort(404)
        return data_child.to_json(orient='records')
@app.route('/getfrequency', methods=['GET'])
def get_frequency():
    if request.method == 'GET':
        file_path = os.path.join(os.getcwd(), 'data','data.csv')
        data_child = DataTimeLine(file_path).get_monthly_payment()
        return data_child.to_json(orient='records')
    else:
        abort(404)
if __name__ == '__main__':
    app.run(debug=True)
