import pandas as pd
import numpy as np
import sqlalchemy as sql

class ReadSQL:
    @staticmethod
    def read_sql_file(address, table_name):
        engine = sql.create_engine(address)
        con = engine.connect()
        
        df = pd.read_sql(f'select * from {table_name}', con)\
            .drop(columns='account')
        
        df.columns = df.columns.str.replace(
            r'((lastsign|last|first)Traffic|last_yandex_direct_click)', '', regex=True
        )
        
        df.columns = df.columns.str.lower()
        
        return df