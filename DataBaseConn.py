import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse


class DatabaseConnection:
    def __init__(self, db_type, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.db_type = db_type
        self.db_url = self.construct_db_url()
        self.engine = self.create_engine()


    def create_engine(self):
        # Create an SQLAlchemy engine using the db_url
        return create_engine(self.db_url)

    def connect(self):
        if self.db_type == "sqlite":
            self.conn = sqlite3.connect(self.db_name)
        elif self.db_type == "postgresql":
            self.conn = self.engine.connect()
            #self.conn = create_engine(self.db_url)

    def disconnect(self):
        if self.conn:
            if self.db_type == "sqlite":
                self.conn.close()
            elif self.db_type == "postgresql":
                if self.conn:
                    #self.conn.close()
                    self.conn = None
                    self.engine.dispose()

    def construct_db_url(self):
        if self.db_type == "sqlite":
            return f"sqlite:///{self.db_name}"
        elif self.db_type == "postgresql":
            user = os.environ.get('POSTGRES_USER')
            password = os.environ.get('POSTGRES_PASSWORD')
            host = os.environ.get('POSTGRES_HOST')
            port = os.environ.get('POSTGRES_PORT', '5432')  # Default port if not specified
            # check environment variables are not empty
            if not user:
                raise ValueError("POSTGRES_USER environment variable is not set")
            if not password:
                raise ValueError("POSTGRES_PASSWORD environment variable is not set")
            if not host:
                raise ValueError("POSTGRES_HOST environment variable is not set")
            if not port:
                raise ValueError("POSTGRES_PORT environment variable is not set")
            
            # Encode special characters in password (if any)
            password = urllib.parse.quote_plus(password)

            return f"postgresql://{user}:{password}@{host}:{port}/{self.db_name}"

    def execute_query(self, query):
        if self.conn:
            
            # wrapping the query in a text() function allows SQLAlchemy to correctly run it
            self.conn.execute(text(query))
            return self.conn.commit()
            
            #return pd.read_sql(query, self.conn)
            
        
    def execute_select_query(self, query):
        if self.conn:
            #cursor = self.conn.cursor()
            #self.conn.execute(query)
            return pd.read_sql(text(query), self.conn)

    def create_table(self, table_name, columns):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.execute_query(text(query))

    def insert_data(self, table_name, data):
        if self.db_type == 'sqlite':
            columns = ", ".join(data.keys())
            values = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in data.values()])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        elif self.db_type == 'postgresql':
            query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES %s"

        self.execute_query(text(query))

    def insert_data_many(self, table_name, data_list, overwrite=False):
        if not self.conn:
            raise ConnectionError("Database connection is not established.")
        
        if self.db_type == 'sqlite':
            # Convert data list to list of dictionaries if it's a DataFrame
            if isinstance(data_list, pd.DataFrame):
                data_list = data_list.to_dict(orient="records")
            
            # Determine whether to overwrite the table
            if overwrite:
                self.execute_query(f"DELETE FROM {table_name}")
            
            # Construct and execute the INSERT query
            column_names = ", ".join(data_list[0].keys())
            placeholders = ", ".join(["?" for _ in data_list[0]])

            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            data_to_insert = [tuple(row.values()) for row in data_list]

            self.conn.executemany(text(query), data_to_insert)

        elif self.db_type == 'postgresql':
            if isinstance(data_list, pd.DataFrame):
                data_list = data_list.to_dict(orient="records")

            if overwrite:
                self.execute_query(f"DELETE FROM {table_name}")

            column_names = ', '.join(data_list[0].keys())
            placeholders = ', '.join(['%s' for _ in data_list[0]])

            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            data_to_insert = [tuple(row.values()) for row in data_list]
            
            self.execute_query(query, data_to_insert)# ... (rest of the class remains the same)
