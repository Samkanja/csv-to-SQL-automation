import sqlite3
from dataclasses import dataclass
from typing import Dict,Any

@dataclass
class DatabaseManager:
    database_name : str
    connetion : sqlite3.Connection = None


    def __post_init__(self):
        self.connetion = sqlite3.connect(self.database_name)

    def __del__(self) ->  None:
        self.connetion.close()

    def _execute(self,statement:str,values=None):
        with self.connetion:
            cursor = self.connetion.cursor()
            cursor.execute(statement,values or [])
            return cursor

    def create_table(self,table_name:str,columns:Dict[str,str]) -> None:
        columns_types = [f'{column_name} {data_type}' for column_name, data_type in columns.items()]
        self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS {table_name}
            ({', '.join(columns_types)});
            '''
        )
    def add(self, table_name:str,data:Dict[str,Any]) -> None:
        placeholders = ', '.join('?' * len(data))
        column_names = ', '.join(data.keys())
        column_values = ', '.join(data.values())
        
        self._execute(
            f'''
            INSERT INTO {table_name}
            ({column_names}) VALUES ({placeholders});
            ''',
            column_values
        )
    

    