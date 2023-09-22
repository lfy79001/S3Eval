import sqlite3
import random
from nltk.corpus import wordnet as wn
from datetime import datetime, timedelta
import string
from value_utils import has_duplicates, random_string, random_strings, random_int, random_float, generate_random_date, generate_random_date_of_birth, random_date_between
import re
from value_utils import read_json
sql_keywords = ['select', 'insert', 'update', 'delete', 'create', 'alter', 'drop', 'truncate', 'from', 'where', 'join', 'on', 'group by', 'order by', 'having', 'distinct', 'as', 'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'null', 'is', 'in', 'between', 'like', 'exists', 'count', 'sum', 'avg', 'max', 'min', 'union', 'intersect', 'except', 'commit', 'rollback', 'savepoint', 'grant', 'revoke', 'index', 'constraint', 'primary key', 'group', 'foreign', 'primary', 'key','foreign key', 'references', 'unique', 'check', 'default','order','values','limit']
# 获取所有名词
nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('n') if re.match(r'^[a-zA-Z]+$', x.name().split('.', 1)[0]) and x.name().split('.', 1)[0] not in sql_keywords}


def read_table(table_path):
    table_name = 'my_table'
    conn = sqlite3.connect(table_path)
    # 创建游标对象
    cursor = conn.cursor()
    # 执行PRAGMA语句获取列名信息
    cursor.execute(f'PRAGMA table_info({table_name})')
    # 获取查询结果
    columns = cursor.fetchall()
    # 提取列名
    header = [column[1] for column in columns]
    types = [column[2] for column in columns]
    # 执行查询，获取内容
    cursor.execute("SELECT * FROM my_table")
    contents = cursor.fetchall()

    # 关闭游标和数据库连接
    cursor.close()
    conn.close()

    return header, contents, types

def insert_random_values(args, table_path,column_number,row_number):
    table_name = 'my_table'

    conn = sqlite3.connect(table_path)
    # 创建游标对象
    cursor = conn.cursor()
    
    # 执行PRAGMA语句获取列名信息
    cursor.execute(f'PRAGMA table_info({table_name})')

    # 获取查询结果
    columns = cursor.fetchall()

    # 提取列名
    column_names = [column[1] for column in columns]
    column_types = [column[2] for column in columns]

    placeholders = ', '.join(['?'] * len(column_names))
    

    rs = RandomString(column_types, row_number)
    for i in range(row_number):
        row_tuple = []
        for j in range(column_number):
            if column_types[j] == 'INT':
                row_tuple.append(random_int())
            elif column_types[j] == 'REAL':
                row_tuple.append(random_float())
            elif column_types[j] == 'DATE':
                row_tuple.append(generate_random_date())
            elif column_types[j] == 'TEXT':
                row_tuple.append(rs.random_generate(i, j))
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", tuple(row_tuple))         
    conn.commit()
    conn.close()
    
class RandomString:
    def __init__(self, column_types, row_number):
        self.column_types = column_types
        self.TEXT_column_number = column_types.count('TEXT')
        self.indices = [i for i, column_type in enumerate(column_types) if column_type == 'TEXT']
        elements = ['double', 'triple', '15th', 'random']
        weights = [0.2, 0.4, 0.2, 0.2]                 ###############################################
        self.random_types = [random.choices(elements, weights)[0] for _ in range(self.TEXT_column_number)]

        self.base_string = []
        for type in self.random_types:
            if type == 'double':
                self.base_string.append(random_strings(2))
            elif type == 'triple':
                self.base_string.append(random_strings(3))
            elif type == '15th':
                self.base_string.append(random_strings(15))
            elif type == 'random':
                self.base_string.append([])
        self.items = []
        for i in range(self.TEXT_column_number):
            if self.random_types[i] in ['double', 'triple', '15th']:
                self.items.append([random.choice(self.base_string[i]) for _ in range(row_number)])
            elif self.random_types[i] in ['random']:
                self.items.append([random_string() for _ in range(row_number)])    
        
    def random_generate(self, row_i, column_j):
        indice = self.indices.index(column_j)
        return self.items[indice][row_i]
             

def generate_random_column_name():
    while True:
        random_noun = random.choice(list(nouns))
        if 3 <= len(random_noun) < 9:
            return random_noun

def generate_random_column_type(weights):
    elements = ['TEXT', 'INT', 'DATE']
    chosen_element = random.choices(elements, weights)[0]
    return chosen_element


def generate_table(args, table_path,column_number, row_number):
    # 读取database config文件 col_min max, 
    database_config = read_json(args.database_config)
    
    
    table_name = 'my_table'
    conn = sqlite3.connect(table_path)
    # 创建游标对象
    cursor = conn.cursor()
    header_name = []
    while True:
        header_name = [generate_random_column_name() for _ in range(column_number)]
        if not has_duplicates(header_name):
            break
    
    header_type = []
    while True:
        header_type = [generate_random_column_type(database_config['text_int_date']) for _ in range(column_number)]
        if header_type.count('TEXT') != 0 and header_type.count('INT') != 0:
            break   

    column_definitions = ', '.join(f'{header_name[i]} {header_type[i]}' for i in range(len(header_name)))
    create_table_query = f'CREATE TABLE {table_name} ({column_definitions})'
    try:
        cursor.execute(create_table_query)
    except:
        print(create_table_query)
        return 0
    conn.commit()
    conn.close()
    return table_path

def execute_sql(db_path, sql):
    conn = sqlite3.connect(db_path)
    # 创建游标对象
    cursor = conn.cursor()

    # 执行查询，获取内容
    cursor.execute(sql)
    contents = cursor.fetchall()

    # 关闭游标和数据库连接
    cursor.close()
    conn.close()
    return contents


def find_element_position(contents, new_answer):
    for row_index, row in enumerate(contents):
        for col_index, element in enumerate(row):
            if str(element) == str(new_answer):
                return row_index, col_index
    return -1, -1  # 表示未找到