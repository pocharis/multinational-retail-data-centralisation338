import boto3
import tabula
import requests
import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
 
        with self.db_connector.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name};")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        

    def retrieve_pdf_data(self, link):
        try:
            pdf_df = tabula.read_pdf(link, stream=True, pages='all')
            
            if len(pdf_df) > 1:
                df = pd.concat(pdf_df)
            else:
                df = pdf_df[0]

            return df
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        

    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        response = requests.get(number_of_stores_endpoint, headers=headers)
        return response.json()['number_stores']
    
    def retrieve_stores_data (self, retrieve_store_endpoint, headers, number_of_stores):
        data = []
        for store_number in range(1, number_of_stores + 1):
            response = requests.get(f"{retrieve_store_endpoint}/{store_number}", headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                data.append(store_data)
            else:
                print(f"Failed to retrieve data for store number {store_number}")


        return pd.DataFrame(data)
    

    def extract_from_s3(self, s3_address):
        link = s3_address.replace('s3://', '').split('/')
        s3 = boto3.client('s3')
        s3.download_file(link[0], link[1], f'./{link[1]}')
        df = pd.read_csv(f'./{link[1]}')

        return df
    
    def extract_date_from_json(self, json_url):
        # Read JSON data from the URL into a DataFrame
        df = pd.read_json(json_url)

        return df

        



if __name__ == "__main__":

    db_connector = DatabaseConnector('db_creds.yaml')

    tables = db_connector.list_db_tables()[1]
    print(tables)
    extractor = DataExtractor(db_connector)

    # user_data_df = extractor.read_rds_table(tables)
    # print(user_data_df)

    # df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # print(df.head())

    # headers = {
    #     "Content-Type": "application/json",
    #     "Authorization": "Bearer your_token_here",
    #     "x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    # }

    # number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    # number_of_stores = extractor.list_number_of_stores(number_of_stores_endpoint, headers)


    # retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    # stores_df = extractor.retrieve_stores_data(retrieve_store_endpoint, headers, number_of_stores)

    # print(stores_df.head())

    s3_address = 's3://data-handling-public/products.csv'
    df = extractor.extract_from_s3(s3_address)
    print(df['weight'].to_list())
