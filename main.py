import pandas as pd
import csv, sys ,os
from database import DatabaseManager

db = DatabaseManager('database.db')

def read_command_line_arg():
    if len(sys.argv) != 2:
        print('onl two arguments required')
        exit()
    if not os.path.exists(sys.argv[1]):
        print('file does not exit')
        exit()
    return {
        'file_path': sys.argv[1]
    }




def create_columns_types_for_table(file):
    data = pd.read_csv(file)
    replacement = {
        'object': 'varchar',
        'float64': 'float',
        'int64':'int',
        'datetime64' : 'timestamp',
        'timedelta64[ns]': 'varchar'

    }
    column_data_types = {column : data_type for column, data_type in zip(data.columns, data.dtypes.replace(replacement))}
    return column_data_types

def table_create():
    arg = read_command_line_arg()
    path = arg['file_path']
    table_name = ''.join(path.split('.')[0]).split('/')[1]
    data = create_columns_types_for_table(path)
    db.create_table(table_name, data)


def add_data_to_database():
    arg = read_command_line_arg()
    path = arg['file_path']
    table_name = ''.join(path.split('.')[0]).split('/')[1]
    data_d = read_csv(path)
    while True:
        data = next(data_d)
        if not any(data):
            break
        db.add(table_name,data)

    

 
def read_csv(file):
    with open(file) as f:
        d_reader = csv.DictReader(f)
        list_of_dict = list(d_reader)
        count = 0
        while len(list_of_dict) > count:
            yield list_of_dict[count]
            count += 1
    

def main():
    table_create()
    add_data_to_database()

if __name__ == '__main__':
    print(main())



