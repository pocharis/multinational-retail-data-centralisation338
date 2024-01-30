import psycopg2
from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:
    def __init__(self, config_file):
        self.config_file = config_file
        self.connection, self.local_connection = self.init_db_engine ()

    def read_db_creds(self):
        with open(self.config_file, 'r') as file:
            creds = yaml.safe_load(file)
            return creds

    def init_db_engine (self):
        creds = self.read_db_creds()

        # Connect to the database
        connection = psycopg2.connect(
            host=creds['RDS_HOST'],
            user=creds['RDS_USER'],
            password=creds['RDS_PASSWORD'],
            dbname=creds['RDS_DATABASE'],
            port=creds['RDS_PORT']
        )
        engine = create_engine(f"{creds['DATABASE_TYPE']}+{creds['DBAPI']}://{creds['USER']}:{creds['PASSWORD']}@{creds['ENDPOINT']}:{creds['PORT']}/{creds['DATABASE']}")#
        local_connection = engine.connect()
        return connection, local_connection

    def list_db_tables(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [table[0] for table in cursor.fetchall()]
            return tables
        
    def upload_to_db(self, data, tb_name):
        try:
            data.to_sql(tb_name, self.local_connection, if_exists='replace')
            print(f"Dataset Successfully Uploaded")
            return True
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

        


