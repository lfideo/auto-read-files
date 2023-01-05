import pandas as pd
import numpy as np
import sqlalchemy as sql

class ReadSQL:
    @staticmethod
    # метод принимает 2 агрумента - адрес базы данных и название таблицы из этой базы
    def read_sql_file(address, table_name):
        engine = sql.create_engine(address)
        con = engine.connect()
        
        # чтение таблицы из БД в dataframe (сразу удаляется столбец 'account')
        df = pd.read_sql(f'select * from {table_name}', con)\
            .drop(columns='account')
        
        # как пример - при выгрузке из Яндекс Метрики, к столбцу с источником припсывается атрибуция
        # строка ниже очищает название атрибуции в стобце и оставляет только Source
        # теперь, чтобы получить доступ к столбцу, не нужно печатать ненужные символы - это удобно
        df.columns = df.columns.str.replace(
            r'((lastsign|last|first)Traffic|last_yandex_direct_click)', '', regex=True
        )
        
        # меняет капитализацю наименований столбцов на lower case
        df.columns = df.columns.str.lower()
        
        # строка ниже возращает готовую таблицу
        return df