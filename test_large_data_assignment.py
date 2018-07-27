import numpy as np
import pandas as pd
import MySQLdb as sql
import MySQLdb.cursors
import json
import unittest
# import businessA_assignment.py


class TestAssignment(unittest.TestCase):

    # This test confirms we're reading correctly from the businessA API & can retrieve the values we want from the JSON
    def test_api_retrieval(self):
        # Port = :8000; Path = /records
        businessA_data_url = 'http://url-to-API-data.us-west-1.elb.amazonaws.com:8000/records'
        # Pagination query parameter
        businessA_param = '?page='
        # data types to assign to columns of page dataframe, with 'uint32' type to save on memory
        businessA_dtypes = {'id': np.uint32, 'name': str, 'address': str, 'birthdate': str, 'sex': str,
                       'job': str, 'company': str, 'emd5': str}

        # Test if the values we're retrieving from the API are correct
        num_businessA_rows = pd.read_json(businessA_data_url+businessA_param+'1',
                                     orient='records', dtype=businessA_dtypes).apply(pd.Series)['num_rows'].iloc[0]
        num_businessA_pages = pd.read_json(businessA_data_url+businessA_param+'1',
                                      orient='records', dtype=businessA_dtypes).apply(pd.Series)['num_pages'].iloc[0]
        self.assertEqual(num_businessA_rows, 636403)
        self.assertEqual(num_businessA_pages, 64)

    # This test is used to confirm that we are reading the SQL database and getting the values we expect from a query
    def test_sql_retrieval(self):
        # firmB connection parameters
        firmB_host = 'data-engineer-rds.moosegozmv.us-west-1.rds.amazonaws.com'
        firmB_port = 3306  # int, not string
        firmB_db = 'firmB'
        firmB_user = 'businessA_read_only'
        _firmB_passwd = 'password'
        # use SSCursor because result set is then stored server side and retrieved row by row with fetches
        firmB_connection = sql.connect(host=firmB_host, port=firmB_port, db=firmB_db,
                                           user=firmB_user,
                                           passwd=_firmB_passwd, cursorclass=MySQLdb.cursors.SSCursor)

        # dual purpose: check we can properly read from sql and the value is what we expect
        cursor = firmB_connection.cursor()
        query = "SELECT COUNT(*) FROM businessA_records_v2"
        cursor.execute(query)
        sql_value = cursor.fetchall()[0][0]
        self.assertEqual(637150, sql_value)

    # This test is used to determine the inner-join union between the sets is working correctly by asserting that a
    # value that's found in line 95 of the assignment code is indeed present in both the individual businessA & firmB
    # data with matching emd5 and job values
    def test_intersection_is_intersection(self):
        # Port = :8000; Path = /records
        businessA_data_url = 'http://url-to-API-data.us-west-1.elb.amazonaws.com:8000/records'
        # Pagination query parameter
        businessA_param = '?page='
        # data types to assign to columns of page dataframe, with 'uint32' type to save on memory
        businessA_dtypes = {'id': np.uint32, 'name': str, 'address': str, 'birthdate': str, 'sex': str,
                       'job': str, 'company': str, 'emd5': str}
        # firmB connection parameters
        firmB_host = 'data-engineer-rds.moosegozmv.us-west-1.rds.amazonaws.com'
        firmB_port = 3306  # int, not string
        firmB_db = 'firmB'
        firmB_user = 'businessA_read_only'
        _firmB_passwd = 'password'
        # use SSCursor because result set is then stored server side and retrieved row by row with fetches
        firmB_connection = sql.connect(host=firmB_host, port=firmB_port, db=firmB_db,
                                           user=firmB_user,
                                           passwd=_firmB_passwd, cursorclass=MySQLdb.cursors.SSCursor)

        # use page 1 because we're sure that the emd5 value we search for in businessA data is on that page
        # The page used is only relevant to retrieve the record.
        businessA_df = pd.read_json(businessA_data_url + businessA_param + '1', orient='records', dtype=businessA_dtypes)['rows'] \
            .apply(pd.Series)
        businessA_job = businessA_df.loc[businessA_df['emd5'] == '035590bff13717e42567dc104a48548b']['job'].iloc[0]
        businessA_emd5 = businessA_df.loc[businessA_df['emd5'] == '035590bff13717e42567dc104a48548b']['emd5'].iloc[0]
        cursor = firmB_connection.cursor()
        # get information for this emd5 in the firmB database
        query = "SELECT * FROM businessA_records_v2 WHERE emd5 = '035590bff13717e42567dc104a48548b'"
        cursor.execute(query)
        sql_result = cursor.fetchall()
        firmB_job = sql_result[0][5]
        firmB_emd5 = sql_result[0][7]
        # check that the jobs are what they should be and they match
        self.assertEqual('Farm manager', businessA_job)
        self.assertEqual('Farm manager', firmB_job)
        self.assertEqual(businessA_job, firmB_job)
        # check that the emd5s are what they should be and they match
        self.assertEqual('035590bff13717e42567dc104a48548b', businessA_emd5)
        self.assertEqual('035590bff13717e42567dc104a48548b', firmB_emd5)
        self.assertEqual(businessA_emd5, firmB_emd5)

    # This test confirms that the JSON list columns loaded from the .CSV file into
    # a dataframe are columns with values that are loadable JSON lists
    def test_read_json_from_csv(self):
        testing_df = pd.read_csv('businessA_firmB_intersection.csv', nrows=10)
        json_value = testing_df['firmB_JSON_list'].iloc[0]
        job_from_json = json.loads(testing_df['firmB_JSON_list'].iloc[0])[0]['job']
        # check that retrieved dataframe value is formatted like a JSON list (JSON array)
        self.assertEqual('[{"job": "Patent attorney", "company": "Richard, Cole and Smith"}]', json_value)
        # check that the string from the column is JSON readable
        self.assertEqual('Patent attorney', job_from_json)


if __name__ == '__main__':
    unittest.main()
    # this code makes the test .py file runnable
    # with $python in the command line

