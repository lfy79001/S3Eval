import sqlite3
import random
from nltk.corpus import wordnet as wn
from datetime import datetime, timedelta
import string
from .value_utils import has_duplicates, random_string, random_strings, random_int, random_float, generate_random_date, generate_random_date_of_birth, random_date_between
import re
from .value_utils import read_json, random_with_weight, random_double
sql_keywords = ['select', 'insert', 'update', 'delete', 'create', 'alter', 'drop', 'truncate', 'from', 'where', 'join', 'on', 'group by', 'order by', 'having', 'distinct', 'as', 'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'null', 'is', 'in', 'between', 'like', 'exists', 'count', 'sum', 'avg', 'max', 'min', 'union', 'intersect', 'except', 'commit', 'rollback', 'savepoint', 'grant', 'revoke', 'index', 'constraint', 'primary key', 'group', 'foreign', 'primary', 'key','foreign key', 'references', 'unique', 'check', 'default','order','values','limit']
# 获取所有名词
nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('n') if re.match(r'^[a-zA-Z]+$', x.name().split('.', 1)[0]) and x.name().split('.', 1)[0] not in sql_keywords}
import pandas as pd
import shutil


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

def insert_random_values(database_config, table_path,column_number,row_number):
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

    rs = RandomString(column_types, row_number, database_config)
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
    def __init__(self, column_types, row_number, database_config):
        self.column_types = column_types
        self.TEXT_column_number = column_types.count('TEXT')
        self.indices = [i for i, column_type in enumerate(column_types) if column_type == 'TEXT']
        elements = ['single', 'double', 'triple', '4th', '5th', '6th', '8th', '10th', '15th', 'random']
        weights = database_config['value_repeat_ratio']                ###############################################
        if "value_repeat_ratio_fix" in database_config.keys():
            self.random_types = database_config["value_repeat_ratio_fix"]
        else:
            self.random_types = [random.choices(elements, weights)[0] for _ in range(self.TEXT_column_number)]
        assert len(self.random_types) == self.TEXT_column_number
        self.base_string = []
        for type in self.random_types:
            if type == 'double':
                self.base_string.append(random_strings(2))
            elif type == 'single':
                self.base_string.append(random_strings(1))
            elif type == 'triple':
                self.base_string.append(random_strings(3))
            elif type == '4th':
                self.base_string.append(random_strings(4))
            elif type == '5th':
                self.base_string.append(random_strings(5))
            elif type == '6th':
                self.base_string.append(random_strings(6))
            elif type == '8th':
                self.base_string.append(random_strings(8))
            elif type == '10th':
                self.base_string.append(random_strings(10))
            elif type == '15th':
                self.base_string.append(random_strings(15))
            elif type == 'random':
                self.base_string.append([])
        self.items = []
        for i in range(self.TEXT_column_number):
            if self.random_types[i] in ['single', 'double', 'triple', '4th', '5th', '6th', '8th', '10th', '15th']:
                self.items.append([random.choice(self.base_string[i]) for _ in range(row_number)])
                # self.items.append([ random_double(self.base_string[i], [14,6]) for _ in range(row_number)])
                # self.items.append(random_double(self.base_string[i], [1,19]))
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


def generate_table(database_config, table_path,column_number, row_number):
    # 读取database config文件 col_min max, 
    
    
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
        if "text_int_date_fix" in database_config.keys():
            header_type = database_config["text_int_date_fix"]
        else:
            header_type = [generate_random_column_type(database_config['text_int_date']) for _ in range(column_number)]

        if (header_type.count('TEXT') != 0 and header_type.count('INT') != 0) or (database_config["text_int_date"].count(0)==2):
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


def transform_output_to_tablestr(header, contents, type='markdown'):
    if contents == [[]]:
        return "None Table"
    table_str = ""
    if type == "markdown":
        df = pd.DataFrame(contents, columns=header)
        table_str = df.to_markdown(headers=header, tablefmt="pipe")
    elif type == "flatten":
        header_string = f'The table have {len(header)} columns: '
        header_string += " | ".join(header) + '\n'
        value_string = ""
        for i, row in enumerate(contents):
            value_string += "row " + str(i+1) + " : "
            row_cell_values = [str(cell_value) if isinstance(cell_value, int) else cell_value.lower()
                            for cell_value in row]
            row_value_string = ""
            for j, value in enumerate(row_cell_values):
                row_value_string += f"{header[j]} is {value}. "
            value_string += row_value_string + '\n'
        table_str = header_string + value_string
    output_str = table_str
    return output_str

def transform_output_to_string(data):
    # 将元组的第一个元素提取出来
    values = [str(item[0]) for item in data]

    # 将列表转换为字符串
    result = ','.join(values)

    # 如果只有一个元组，则只输出该元组的第一个元素
    if len(data) == 1:
        result = values[0]
    return result


def generate_intermedium_table(original_db_path, contents):
    
    new_db_path = '/home/aiops/liuqian/fangyu/db/template.db'

    # 复制数据库文件到新路径
    shutil.copyfile(original_db_path, new_db_path)

    # 连接到新路径的数据库
    conn = sqlite3.connect(new_db_path)

    # 创建游标对象
    cursor = conn.cursor()

    # 清空表格
    cursor.execute("DELETE FROM my_table")

    for content in contents:
        cursor.execute("INSERT INTO my_table VALUES {}".format(tuple(content)))

    # 提交更改
    conn.commit()

    # 关闭连接
    conn.close()
    return new_db_path