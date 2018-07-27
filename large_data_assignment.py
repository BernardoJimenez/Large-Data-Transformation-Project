import numpy as np
import pandas as pd
import MySQLdb as sql
import MySQLdb.cursors
from datetime import timedelta
import time
import json
import csv

# monotonic used to avoid issues with using local time such as daylight savings
start_time = time.monotonic()


# Port = :8000; Path = /records
businessA_data_url = 'http://url-to-API-data.us-west-1.elb.amazonaws.com:8000/records'
# Pagination query parameter
businessA_param = '?page='
# data types to assign to columns of page dataframe, with 'uint32' type to save on memory
businessA_dtypes = {'id': np.uint32, 'name': str, 'address': str, 'birthdate': str, 'sex': str,
                    'job': str, 'company': str, 'emd5': str}

# simpler ways to get row and page numbers from JSON, but I'm sure this works and it returns the values as int64
num_businessA_rows = pd.read_json(businessA_data_url+businessA_param+'1',
                                  orient='records', dtype=businessA_dtypes).apply(pd.Series)['num_rows'].iloc[0]
num_businessA_pages = pd.read_json(businessA_data_url+businessA_param+'1',
                                   orient='records', dtype=businessA_dtypes).apply(pd.Series)['num_pages'].iloc[0]


# firmB connection parameters
firmB_host = 'data-engineer-rds.moosegozmv.us-west-1.rds.amazonaws.com'
firmB_port = 3306  # int, not string
firmB_db = 'firmB'
firmB_user = 'businessA_read_only'
_firmB_passwd = 'password'

# use SSCursor because result set is then stored server side and retrieved row by row with fetches
firmB_connection = sql.connect(host=firmB_host, port=firmB_port, db=firmB_db, user=firmB_user,
                                    passwd=_firmB_passwd, cursorclass=MySQLdb.cursors.SSCursor)


# variable to add up total intersecting emd5 values with the same job title
total_same_job_title = 0
# variable to add up the total number of rows in the intersection businessA and firmB
total_intersection_rows = 0

# 'with' keyword wraps code in a try, finally block to avoid errors from file not being found/created yet
with open('businessA_firmB_intersection.csv', 'w') as csv_file:
    # create header for blank .CSV file to hold our intersection
    csv.writer(csv_file).writerow(["emd5", "businessA_JSON_list", "firmB_JSON_list"])

# open the file created in the preceding code in append mode
csv_file = open('businessA_firmB_intersection.csv', 'a')

# iterate through all pages of businessA data from API
for _businessA_page_num in range(1, (num_businessA_pages+1)):

    # save the page data from the API to a pandas dataframe
    businessA_df = pd.read_json(businessA_data_url + businessA_param + str(_businessA_page_num), orient='records',
                           dtype=businessA_dtypes)['rows'].apply(pd.Series)[['emd5', 'job', 'company']]

    # edit the emd5 column from the data frame into a usable format for a SQL query
    businessA_emd5_vals = '(' + str(businessA_df['emd5'].tolist()).strip("[]") + ')'  # json.dumps simpler?

    # select from the firmB database users with the same emd5 value as users in the page dataframe
    sql_query = "SELECT emd5, job, company FROM businessA_records_v2 WHERE emd5 IN {}".format(businessA_emd5_vals)
    # create a dataframe from the records returned by the query
    sql_df = pd.read_sql_query(sql_query, firmB_connection)

    # drop rows from page dataframe that did not find a match in the firmB database
    businessA_df = businessA_df.loc[businessA_df['emd5'].isin(sql_df['emd5'])]

    # convert job and company columns into JSON with key,value pairs
    businessA_json_vals = json.loads(businessA_df.drop(['emd5'], axis=1).to_json(orient='records'))
    # create an empty python list to hold the formatted JSON values
    json_list = []
    # create a JSON list holding job and company key,value pairs for each row in the businessA page dataframe
    for k in range(len(businessA_df.index)):
        # replace single quotes with double, as required by JSON syntax, and add brackets to make JSON array(JSON list)
        a_row = ('[' + str(businessA_json_vals[k]) + ']').replace("'", '"')  # what to insert
        # add formatted string to the json list created earlier
        json_list.append(a_row)
    # convert complete list of JSON lists to a pandas series to add as a column with matching index to page dataframe
    businessA_df['businessA_JSON_list'] = pd.Series(json_list, index=businessA_df.index)

    # Repeat entire process to create a similar firmB_JSON_list column for the firmB dataframe
    sql_json_vals = json.loads(sql_df.drop(['emd5'], axis=1).to_json(orient='records'))
    json_list = []
    for k in range(len(sql_df.index)):  # 0,
        a_row = ('[' + str(sql_json_vals[k]) + ']').replace("'", '"')  # what to insert
        json_list.append(a_row)
    sql_df['firmB_JSON_list'] = pd.Series(json_list, index=sql_df.index)
    # actually adding the column with proper name would be complicated if were a func definition instead

    # get the number of users from the businessA page dataframe with matching emd5 and job title in the firmB DB
    same_job_and_emd5 = len(pd.merge(sql_df, businessA_df, on=['emd5', 'job']).index)

    # drop the job and company rows from the respective dataframes, leaving us with only our desired data
    sql_df.drop(['job', 'company'], axis=1, inplace=True)
    businessA_df.drop(['job', 'company'], axis=1, inplace=True)

    # merge the dataframes with an inner join to make a three column dataframe with the relevant data
    intersection_df = pd.merge(sql_df, businessA_df, on=['emd5'], how='inner')
    # get the number of rows in the intersection dataframe
    intersection_rows = len(intersection_df.index)

    # add number of users from this page with same emd5 and job to the total
    total_same_job_title += same_job_and_emd5
    # add the number of rows in this intersection dataframe to the total
    total_intersection_rows += intersection_rows

    # append the created interaction dataframe rows to the .CSV file
    intersection_df.to_csv('businessA_firmB_intersection.csv', mode='a', header=False, index=False)
# close the .CSV file
csv_file.close()


# calculate the total number of users found only in the businessA data set
unique_businessA_users = num_businessA_rows - total_intersection_rows
# create a cursor to perform SQL queries on the firmB database
cursor = firmB_connection.cursor()
# create a query to get the total number of users in the firmB database
query = "SELECT COUNT(*) FROM businessA_records_v2"
# run the SQL query on the database by using the cursor
cursor.execute(query)
# calculate the total number of users found only in the firmB data set
unique_firmB_users = cursor.fetchall()[0][0] - total_intersection_rows
# calculate the total percent of users in the intersection with matching job titles and convert to string
percent_diff_job_title = str(100.0 * (1.0 - float((total_same_job_title / total_intersection_rows)))) + '%'

firmB_connection.close()  # generally safe to let SQL connection close implicitly, but best practice to be explicit


# use 'with' keyword again to make 'results.txt' file
with open('results.txt', 'w') as results_file:
    # get time at output file creation
    end_time = time.monotonic()
    # calculate the runtime to output
    results_file.write(str(timedelta(seconds=end_time - start_time)) + '\n')
    # convert the relevant values to write to 'results.txt' to string values and/or add a newline
    results_file.write(str(total_intersection_rows) + '\n')
    results_file.write(str(unique_businessA_users) + '\n')
    results_file.write(str(unique_firmB_users) + '\n')
    results_file.write(percent_diff_job_title + '\n')
    # iterate through the middle 10 rows of the final .CSV intersection file and add them to 'results.txt'
    for index, row in pd.read_csv('businessA_firmB_intersection.csv',
                                  skiprows=range(1, int(((total_intersection_rows/2) - 5))), nrows=10).iterrows():
        # write rows from pandas dataframe created from middle 10 .CSV records to 'results.txt
        results_file.write(row["emd5"] + ',' + row["businessA_JSON_list"] + ',' + row["firmB_JSON_list"] + '\n')
    # create the SQL query that could hold the data in the businessA_firmB_intersection.csv file
    results_file.write('CREATE TABLE csv_output_table(\n\tid int(11) unsigned NOT NULL AUTO_INCREMENT,\n\t' +
                       'emd5 varchar(32) DEFAULT NULL,\n\tbusinessA_JSON_list json DEFAULT NULL,\n\t' +
                       'firmB_JSON_list json DEFAULT NULL,\n\tPRIMARY KEY (id));')
