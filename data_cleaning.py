import re
import pandas as pd
import numpy as np
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning: 
    def __init__(self, data):
        
        self.data = data

    def clean_user_data(self):
        # Handle NULL values

        print(len(self.data), self.data.isnull().any(axis=1).sum(), self.data.columns)
        self.data['date_of_birth'] = pd.to_datetime(self.data['date_of_birth'], errors='coerce')


        return self.data
    
    def clean_card_data(self):
        # Handle NULL values
        self.data = self.data[['card_number', 
                               'expiry_date', 
                               'card_provider',
                               'date_payment_confirmed']
                               ].copy()
        self.data['card_number'] = self.data['card_number'].astype('str')
        
        self.data = self.data.dropna(subset=['card_number'])
        print('removing regex')
        self.data['card_number'] = self.data['card_number'].str.replace('^\\?+', '', regex=True)

        
        self.data['date_payment_confirmed'] = pd.to_datetime(self.data['date_payment_confirmed'], errors='coerce')


        return self.data
    
    def clean_store_data(self):
        # Handle NULL values
        

        # self.data = self.data.dropna() 
        self.data.drop(['index'], axis=1, inplace=True)


        return self.data
    
    def convert_product_weights(self):


        conversion_key = {'kg': 1, 'g':0.001, 'ml':0.001}

        def remove_special_characters(input_string):
            pattern = r'[^a-zA-Z0-9 ]'

            # Substitute the matched characters with an empty string
            cleaned_string = re.sub(pattern, '', input_string)

            return cleaned_string

        # method will take a text like 200g and return float(200) and unit(g)
        def get_number(text):
            
            if 'kg' in text:
                number = float(text.replace('kg',''))
                return number, 'kg'
            elif 'g' in text:
                number = float(text.replace('g',''))
                return number, 'g'

            elif 'ml' in text:
                number = float(text.replace('ml',''))
                return number, 'ml'
            else:
                return 0, 'kg'



        def convert_to_kg(text):
            if pd.isna(text):
                return 0
            text = remove_special_characters(text)

            if 'x' in str(text):
                text = text.split('x')
                number, unit = get_number(text[1])

                final_figure = float(conversion_key[unit] * (float(text[0]) * number))
            else:
                number, unit = get_number(text)
                final_figure = float(conversion_key[unit] * number)
                return final_figure

        # print(self.data['weight'].to_list())
        self.data['weight'] = self.data['weight'].apply(convert_to_kg)
        self.data.rename(columns={ 'weight': "weight_kg" }, inplace = True)
        
        return self.data
    


    def clean_products_data(self):
              

        # self.data = self.data.dropna() 
        self.data.drop(['Unnamed: 0'], axis=1, inplace=True)

        return self.data

    def clean_orders_data(self):     

        # self.data = self.data.dropna() 
        self.data.drop(['index','level_0','first_name', 'last_name', '1'], axis=1, inplace=True)

        return self.data
    

    def clean_time_data(self):     

        return self.data
     

if __name__ == "__main__":

    db_connector = DatabaseConnector('db_creds.yaml')
    tables = db_connector.list_db_tables()
    print(tables)

    extractor = DataExtractor(db_connector)


    ### extract the users data
    user_data_df = extractor.read_rds_table(tables[1])

    #### clean the users data
    data_cleaner = DataCleaning(user_data_df)
    cleaned_data = data_cleaner.clean_user_data()

    #### save users data
    db_connector.upload_to_db(cleaned_data, 'dim_users') 



    #### extract orders data
    order_data_df = extractor.read_rds_table(tables[2])

    #### clean orders data
    data_cleaner = DataCleaning(order_data_df)
    cleaned_data = data_cleaner.clean_orders_data ()
    print(len(cleaned_data))
    print(cleaned_data.head())

    #### save orders data
    db_connector.upload_to_db(cleaned_data, 'orders_table') 
    
    
    
    #### extract time data
    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    time_data_df = extractor.extract_date_from_json(json_url)

    #### clean time data
    data_cleaner = DataCleaning(time_data_df)
    cleaned_data = data_cleaner.clean_time_data ()
    print(cleaned_data.head())

    db_connector.upload_to_db(cleaned_data, 'dim_date_times') #save time data

    
    
    #### extract the card data
    card_data_df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    #### clean the cards data
    data_cleaner = DataCleaning(card_data_df)
    cleaned_data = data_cleaner.clean_card_data()
    print(cleaned_data)

    #### save card data
    db_connector.upload_to_db(cleaned_data, 'dim_card_details') 



    ### header for the stores data
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer your_token_here",
        "x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    #### get the number of stores used in extracting all the stores
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_of_stores = extractor.list_number_of_stores(number_of_stores_endpoint, headers)


    #### extract the store data
    retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    stores_df = extractor.retrieve_stores_data(retrieve_store_endpoint, headers, number_of_stores)

    #### clean the stores data
    data_cleaner = DataCleaning(stores_df)
    cleaned_data = data_cleaner.clean_store_data()
    print(cleaned_data.head())

    db_connector.upload_to_db(cleaned_data, 'dim_store_details') #save store data



    #### extract the products data
    s3_address = 's3://data-handling-public/products.csv'
    products_df = extractor.extract_from_s3(s3_address)

    #### clean the products data
    data_cleaner = DataCleaning(products_df)
    cleaned_weight = data_cleaner.convert_product_weights()
    print(cleaned_weight.head())

    data_cleaner = DataCleaning(cleaned_weight)
    cleaned_data = data_cleaner.clean_products_data()
    print(cleaned_data.head())

    db_connector.upload_to_db(cleaned_data, 'dim_products') #save products data 
    
    

    









