import os

import sqlite3 
import pandas as pd

from config import choose_expe


class DB_connection():
    def __init__(self, expe = None):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
        if expe == "expe_1":
            db_path = os.path.join(
                project_root,
                "data","expe_1" ,"database.db"
            )
        elif expe == "expe_2":
            db_path = os.path.join(
                project_root,
                "data","expe_2" ,"database.db"
            )
        elif expe is None:
            if choose_expe == "expe_1":
                db_path = os.path.join(
                    project_root,
                    "data","expe_1" ,"database.db"
                )
            elif choose_expe == "expe_2":
                db_path = os.path.join(
                    project_root,
                    "data","expe_2" ,"database.db"
            )
        self.connection = sqlite3.connect(db_path, timeout=10)
    
    def select(self,query):
        return pd.read_sql_query(query, self.connection)

    
    def select_single_value(self,query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]
    
    def drop_table(self,table_name):
        delete_bool = input(f"Are you sure you want to delete the table {table_name} ? type yes and enter to continue the process ")
        if delete_bool == 'yes':
            cursor = self.connection.cursor()
            query  = "DROP TABLE IF EXISTS " + table_name
            cursor.execute(query)
            print('Table '+ table_name + ' has been deleted')

    def save_df(self,df,table_name):
        df.to_sql(table_name, self.connection, if_exists="replace", index =False)

    def create_table(self,query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
    

    def execute_query(self, query, params=None):
        try:
            cur = self.connection.cursor()
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            self.connection.rollback()

    def close(self):
        self.connection.close()


            
