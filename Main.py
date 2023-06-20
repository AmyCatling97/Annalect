import requests
import pandas as pd
import pyodbc

from Config import *


class ApiCall:
    def __init__(self, url):
        self.url = url
        self.df = None      # The dataframe is initialised as none and will be created after a successful API call
        self.make_call()

    def make_call(self):
        response = requests.get(self.url)
        if response.status_code == 200:     # Calls the API, if successful passes the response to create a dictionary
            print("Successful request")
            self.format_response(response.json())
        else:
            print(f"Error: {response.status_code} occurred with your API request")

    def format_response(self, data):
        self.dict_items = []
        for item in data:   # iterate though and add the required fields into a temp dictionary
            temp_dict = {
                "first_name": item['first_name'],
                "last_name": item['last_name'],
                "gender": item['gender'],
                "country": item['address']['country']
            }
            self.dict_items.append(temp_dict)
        self.create_df()

    def create_df(self):
        self.df = pd.DataFrame(self.dict_items)  # Creates a dataframe from the temp dictionary

    def get_dataframe(self):
        return self.df


class LoadData:
    def __init__(self, data_frame):
        self.conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server +
            ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        # Uses data from the config file to connect to an SQL server
        self.data_frame = data_frame
        self.load_to_azure()

    def load_to_azure(self):
        cursor = self.conn.cursor()
        for index, row in self.data_frame.iterrows():
            cursor.execute(
                "INSERT INTO dbo.api_data (first_name, last_name, gender, country) VALUES (?, ?, ?, ?)",
                row['first_name'], row['last_name'], row['gender'], row['country'])
        self.conn.commit()


api_call = ApiCall('https://random-data-api.com/api/users/random_user?size=100')
data_frame = api_call.get_dataframe()
print(data_frame)

load_data = LoadData(data_frame)
