import os
import glob
import pandas as pd
import sqlite3
import argparse
from tqdm import tqdm
import shutil

def get_column_type(data):
    column_types = []
    for column in data.columns:
        column_data = data[column]
        if column_data.dtype == 'object':
            try:
                column_data.astype(float)
                column_types.append('REAL')
            except ValueError:
                column_types.append('TEXT')
        elif column_data.dtype == 'int64':
            column_types.append('INTEGER')
        elif column_data.dtype == 'float64':
            column_types.append('REAL')
    return column_types

def convert_to_db(file_path, db_name):
    table_name = "my_table"
    if file_path.endswith('.csv'):
        data = pd.read_csv(file_path)
    else:
        data = pd.read_excel(file_path)

    column_types = get_column_type(data)
    create_table_query = f"CREATE TABLE {table_name} ({', '.join(f'{col} {type}' for col, type in zip(data.columns, column_types))})"

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    data.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    
def get_file_paths(directory):
    file_paths = []
    pattern = os.path.join(directory, '*')  # 匹配指定目录下的所有文件

    for file_path in glob.glob(pattern):
        if os.path.isfile(file_path):  # 确保是文件路径，而非目录路径
            file_paths.append(file_path)

    return file_paths
    
    
def main(args):
    if os.path.exists(args.db_path):
        shutil.rmtree(args.db_path)
        os.makedirs(args.db_path)
    else:
        os.makedirs(args.db_path)

    file_paths = get_file_paths(args.csv_path)
    file_number = len(file_paths)
    for i in tqdm(range(file_number)):
        db_name = os.path.join(args.db_path, f"table{i}.db")
        convert_to_db(file_paths[i], db_name)
    



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--db_path', type=str, default='./db/db27')
    parser.add_argument('--csv_path', type=str, default='/home/aiops/liuqian/fangyu/excel')
    args = parser.parse_args()

    main(args)
