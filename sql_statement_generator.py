import pandas as pd
from typing import List


class trading_data_statement_generator:
    def __init__(self):
        pass

    def add_row(self, stock_no:int, values:dict):
        ''' 
        1. keys of values are date, open, close, high, low, vol.
        2. table name = f'_{stock_no}'
        3. date is of type datetime.date
        '''
        statement = """INSERT OR REPLACE INTO _{} (Date, Open, Close, High, Low, Volume)
                            VALUES ("{}",{}, {}, {}, {}, {});""".format(stock_no, values['date'], 
                                                                        values['open'], values['close'], values['high'],
                                                                        values['low'], values['vol'])
        return statement
    
    def add_rows(self, stock_no:int, rows:List[dict]):
        n_rows = len(rows)
        values = []
        for row_i, row in enumerate(rows):
            if row_i!=n_rows - 1:
                row_str = """("{}",{}, {}, {}, {}, {}),\n""".format(row['date'], row['open'], row['close'], row['high'],row['low'], row['vol'])
            else:
                row_str = """("{}",{}, {}, {}, {}, {});""".format(row['date'], row['open'], row['close'], row['high'],row['low'], row['vol'])

            values.append(row_str)

        statement = """INSERT OR REPLACE INTO _{} (Date, Open, Close, High, Low, Volume) \n""".format(stock_no)
        values[0] = "VALUES " + values[0]
        for value in values:
            statement = statement + value

        return statement

    def create_table(self, stock_no:int):
        '''with 'Date' as the primary key'''

        statement = """ CREATE TABLE IF NOT EXISTS _{}(
        Date DATE PRIMARY KEY,
        Open FLOAT,
        Close FLOAT,
        High FLOAT,
        Low FLOAT, 
        Volume FLOAT
        );""".format(stock_no)

        return statement

    def list_tables(self):
        statement = """SELECT name FROM sqlite_master  
                    WHERE type='table';"""
        return statement
    
    def drop_table(self, table_name:str):
        statement = """DROP TABLE IF EXISTS {}""".format(table_name)
        return statement

    def select_latest_date_of_the_table(self, table_name):
        statement = """SELECT Date FROM {} ORDER BY Date DESC LIMIT 1""".format(table_name)
        return statement

    def select_table(self, table_name = '_9934'):
        statement = """SELECT * FROM {};""".format(table_name)
        return statement
    
    def select_last_n_rows_of_the_table(self, table_name:str, n_rows:int):
        statement = """SELECT * FROM {} ORDER BY Date DESC LIMIT {}""".format(table_name, n_rows)
        return statement
    
    def select_row_at(self, table_name:str, dt='2024-11-21'):
        statement = """SELECT * FROM {} WHERE Date='{}'""".format(table_name, dt)
        return statement
    
    def select_row_before_some_date(self, table_name:str, dt='2024-11-21'):
        statement = """SELECT * FROM {} WHERE 
                    rowid<=(SELECT rowid
                            FROM {}
                            WHERE Date='{}'
                            ) """.format(table_name, table_name, dt)
        return statement
    
    def select_rows_between_a_period(self, table_name='_1101', start='2024-11-01', end='2024-11-03'):
        statement = """SELECT * FROM {} WHERE
                    Date>='{}' AND Date<='{}' 
                    """.format(table_name, start, end)
        
        return statement