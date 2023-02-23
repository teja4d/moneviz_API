import csv
import datetime
#df = pd.read_csv('data.csv', header=0)
#df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], format='%d/%m/%Y')
# set the file name and open the file
filename = "data.csv"
#change transaction fromat from ='%d/%m/%Y' to '%Y-%m-%d'

with open(filename, 'r') as file:
    # read the CSV file
    reader = csv.reader(file)
    #add payment frequency header row to the csv file
    
    #
    # skip the header row
    header = next(reader)
    # initialize the last transaction date and description
    last_transaction_date = None
    last_transaction_desc = None
    # initialize a dictionary to keep track of transaction frequency
    transaction_freq = {}
    # loop through each row of the file
    for row in reader:
        # get the transaction date and description from the row '%d/%m/%Y' to '%Y-%m-%d'

        transaction_date = datetime.datetime.strptime(row[0], '%d/%m/%Y').date()
        transaction_desc = row[1]
        # check if the transaction description has been seen before
        if transaction_desc not in transaction_freq:
            # if it hasn't been seen, set the frequency to 'irregular'
            transaction_freq[transaction_desc] = 'irregular'
        # check if the transaction date is within 28-30 days of the last transaction date
        if last_transaction_date is not None and (transaction_date - last_transaction_date).days in range(28, 31):
            # if it is, update the frequency to 'monthly'
            transaction_freq[transaction_desc] = 'monthly'
        # update the last transaction date and description
        last_transaction_date = transaction_date
        last_transaction_desc = transaction_desc

# update the CSV file with the new frequency information
with open(filename, 'r') as file:
    # read the CSV file
    reader = csv.reader(file)
    # skip the header row
    header = next(reader)
    # create a list of dictionaries for each row, with updated frequency information
    rows = []
    for row in reader:
        row_dict = {header[i]: row[i] for i in range(len(header))}
        transaction_desc = row_dict['Transaction Description']
        row_dict['Payment Frequency'] = transaction_freq.get(transaction_desc, 'irregular')
        rows.append(row_dict)

# overwrite the CSV file with the updated rows
with open(filename, 'w', newline='') as file:
    # write the header row
    writer = csv.DictWriter(file, fieldnames=header)
    writer.writeheader()
    # write the updated rows
    writer.writerows(rows)
