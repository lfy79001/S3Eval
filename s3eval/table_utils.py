import sqlite3
import random, os
from nltk.corpus import wordnet as wn
from datetime import datetime, timedelta
import string
from .value_utils import has_duplicates, random_string, random_strings, random_int, random_float, generate_random_date, generate_random_date_of_birth, random_date_between
import re
from .value_utils import read_json, random_with_weight, random_double
sql_keywords = ['select', 'insert', 'update', 'delete', 'create', 'alter', 'drop', 'truncate', 'from', 'where', 'join', 'on', 'group by', 'order by', 'having', 'distinct', 'as', 'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'null', 'is', 'in', 'between', 'like', 'exists', 'count', 'sum', 'avg', 'max', 'min', 'union', 'intersect', 'except', 'commit', 'rollback', 'savepoint', 'grant', 'revoke', 'index', 'constraint', 'primary key', 'group', 'foreign', 'primary', 'key','foreign key', 'references', 'unique', 'check', 'default','order','values','limit']
# Get all English nouns
nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('n') if re.match(r'^[a-zA-Z]+$', x.name().split('.', 1)[0]) and x.name().split('.', 1)[0] not in sql_keywords}
import pandas as pd
import shutil
from transformers import AutoTokenizer


def get_table_length(table_path, tokenizer_path, format):
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
    
    header, contents, _ = read_table(table_path)
    
    table_str = ""
    if format == 'flatten':
        header_string = f'The table have {len(header)} columns: '
        header_string += " | ".join(header) + '\n'
        value_string = ""
        for i, row in enumerate(contents):
            value_string += "row " + str(i+1) + " : "
            row_cell_values = [str(cell_value).lower() if isinstance(cell_value, str) else str(cell_value) if isinstance(cell_value, int) else '' for cell_value in row]
            row_value_string = ""
            for j, value in enumerate(row_cell_values):
                row_value_string += f"{header[j]} is {value}. "
            value_string += row_value_string + '\n'
        table_str = header_string + value_string  
    elif format == 'markdown':
        df = pd.DataFrame(contents, columns=header)
        table_str = df.to_markdown(headers=header, tablefmt="pipe", index=False)   
    table_length = len(tokenizer.tokenize(table_str)) 
    return table_length
    
    
def generate_database_config(database_config, context_length, tokenizer_path, context_length_format, db_path):
    
    database_config = read_json(database_config) 
    # the row range and column range of tables
    column_numbers = list(range(database_config['col_min'], database_config['col_max']+1))
    row_numbers = list(range(database_config['row_min'], database_config['row_max']+1))


    # if args.context_length is given, generate data based on the specified number of tokens
    if context_length != 0 and tokenizer_path:        
        database_config['col_min'] = 5
        database_config['col_max'] = 5
        
        result = context_length / 80
        remainder = result % 5
        if remainder >= 2.5:
            result = (result // 5 + 1) * 5
        else:
            result = (result // 5) * 5
        
        row_last = int(result)
        
        ###################### Sampling to find the most suitable number of table columns
        while True:
            database_config['row_min'] = row_last
            database_config['row_max'] = row_last
            
            column_numbers = list(range(database_config['col_min'], database_config['col_max']+1))
            row_numbers = list(range(database_config['row_min'], database_config['row_max']+1))
            
            
            table_name = 'table_try' 
            table_path = os.path.join(db_path, table_name + '.db')
            
            column_number = random.choice(column_numbers)
            row_number = random.choice(row_numbers)
            
            while True:
                output = generate_table(database_config, table_path,column_number,row_number)
                if output != 0:
                    break   
            
            insert_random_values(database_config, table_path, column_number, row_number)

            table_length = get_table_length(table_path, tokenizer_path, context_length_format)
            
            if table_length > context_length:
                row_last -= 5
                break
            else:
                row_last += 5
            delete_table(table_path)
        #########################
        delete_table(table_path)

    print(f"database_config: {database_config}\ncolumn_numbers: {column_numbers}, row_numbers: {row_numbers}")
    return database_config, column_numbers, row_numbers

                
            
        
        



def read_table(table_path, table_name='my_table'):
    conn = sqlite3.connect(table_path)
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info({table_name})')

    columns = cursor.fetchall()

    header = [column[1] for column in columns]
    types = [column[2] for column in columns]

    cursor.execute(f"SELECT * FROM {table_name}")
    contents = cursor.fetchall()

    cursor.close()
    conn.close()

    return header, contents, types

def insert_random_values(database_config, table_path,column_number,row_number, table_name="my_table"):
    conn = sqlite3.connect(table_path)
    cursor = conn.cursor()
    
    cursor.execute(f'PRAGMA table_info({table_name})')

    columns = cursor.fetchall()

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
        weights = database_config['value_repeat_ratio']   
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
            elif self.random_types[i] in ['random']:
                self.items.append([random_string() for _ in range(row_number)])    
        
    def random_generate(self, row_i, column_j):
        indice = self.indices.index(column_j)
        return self.items[indice][row_i]
    
    def output_rows(self):
        row_tuple = []
        column_types = self.column_types
        column_number = len(column_types)
        for j in range(column_number):
            if column_types[j] == 'INT':
                row_tuple.append(random_int())
            elif column_types[j] == 'REAL':
                row_tuple.append(random_float())
            elif column_types[j] == 'DATE':
                row_tuple.append(generate_random_date())
            elif column_types[j] == 'TEXT':
                row_tuple.append(random_string())
        return row_tuple
    
def output_random_cells(type):
    if type == 'INT':
        output = random_int()
    elif type == 'REAL':
        output = random_float()
    elif type == 'DATE':
        output = generate_random_date()
    elif type == 'TEXT':
        output = random_string()
    return output
            

def generate_random_column_name():
    while True:
        random_noun = random.choice(list(nouns))
        if 3 <= len(random_noun) < 9:
            return random_noun

def generate_random_column_type(weights):
    elements = ['TEXT', 'INT', 'DATE']
    chosen_element = random.choices(elements, weights)[0]
    return chosen_element


def generate_table(database_config, table_path,column_number, row_number, header_type=None):
    table_name = 'my_table'
    conn = sqlite3.connect(table_path)
    
    cursor = conn.cursor()
    header_name = []
    while True:
        header_name = [generate_random_column_name() for _ in range(column_number)]
        if not has_duplicates(header_name):
            break
    
    if header_type == None:
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


def generate_new_table(database_config, table_path,column_number, row_number, table_name='new_table', header_name=None, header_type=None):
    conn = sqlite3.connect(table_path)
    
    cursor = conn.cursor()
    if header_name == None:
        header_name = []
        while True:
            header_name = [generate_random_column_name() for _ in range(column_number)]
            if not has_duplicates(header_name):
                break
    
    if header_type == None:
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
        import pdb; pdb.set_trace()
        return 0
    conn.commit()
    conn.close()
    return table_path



def generate_subtable(table_path, table_name, header_name, contents, header_type):
    conn = sqlite3.connect(table_path)
    cursor = conn.cursor()
    column_definitions = ', '.join(f"\"{header_name[i]}\" {header_type[i]}" for i in range(len(header_name)))
    create_table_query = f'CREATE TABLE {table_name} ({column_definitions})'
    try:
        cursor.execute(create_table_query)
    except:
        import pdb; pdb.set_trace()
        print(create_table_query)
        return 0
    conn.commit()
    conn.close()

    conn = sqlite3.connect(table_path)
    cursor = conn.cursor()
    placeholders = ', '.join(['?'] * len(header_name))
    for i in range(len(contents)):
        row_tuple = contents[i]
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", tuple(row_tuple))         

    conn.commit()
    conn.close()
    


def execute_sql(db_path, sql):
    conn = sqlite3.connect(db_path)

    cursor = conn.cursor()

    try:
        cursor.execute(sql)
    except:
        import pdb; pdb.set_trace()

    contents = cursor.fetchall()
    
    conn.commit()

    cursor.close()
    conn.close()
    return contents


def find_element_position(contents, new_answer):
    for row_index, row in enumerate(contents):
        for col_index, element in enumerate(row):
            if str(element) == str(new_answer):
                return row_index, col_index
    return -1, -1  # not found


def transform_output_to_tablestr(header, contents, type='markdown'):
    if contents == []:
        return "None Table"
    if contents == [[]]:
        return "None Table"
    table_str = ""
    if type == "markdown":
        df = pd.DataFrame(contents, columns=header)
        table_str = df.to_markdown(headers=header, tablefmt="pipe", index=False)
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

    if len(data) == 0:
        result = ""
    elif len(data) == 1:
        result = data[0][0]
    else:
        if len(data[0]) == 1:
            values = [str(item[0]) for item in data]
            result = ', '.join(values)
        elif len(data[0]) > 1:
            result = ", ".join([str(item) for item in data])
    return result


def generate_intermedium_table(original_db_path, contents):
    
    new_db_path = '/home/aiops/liuqian/fangyu/db/template.db'

    shutil.copyfile(original_db_path, new_db_path)

    conn = sqlite3.connect(new_db_path)

    cursor = conn.cursor()
    cursor.execute("DELETE FROM my_table")

    for content in contents:
        cursor.execute("INSERT INTO my_table VALUES {}".format(tuple(content)))

    conn.commit()

    conn.close()
    return new_db_path


def delete_table(table_path):
    try:
        os.remove(table_path)
        print(f"The table does not meet the requirements. Resampling ......")
    except FileNotFoundError:
        print(f"The file {table_path} does not exist.")
    except PermissionError:
        print(f"Permission denied to delete the file {table_path}.")
    except Exception as e:
        print(f"An error occurred while deleting the file {table_path}: {str(e)}") 
        
        
        
def markdown_table(header, contents):      
    df = pd.DataFrame(contents, columns=header)
    table_str = df.to_markdown(headers=header, tablefmt="pipe", index=False)
    return table_str


def delete_single_table(table_path, table_name):
    conn = sqlite3.connect(table_path)
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE {table_name}')
    cursor.close()
    conn.close()
    
def get_database_tables(table_path):
    conn = sqlite3.connect(table_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
    
    contents = cursor.fetchall()
    
    cursor.close()
    conn.close()

    return [item[0] for item in contents]


def get_output_table(table_path, sql):
    conn = sqlite3.connect(table_path)
    df = pd.read_sql(sql, conn)
    output = df.to_markdown(tablefmt="pipe", index=False)
    conn.close()
    return output


def markdown_table(header, contents):      
    df = pd.DataFrame(contents, columns=header)
    table_str = df.to_markdown(headers=header, tablefmt="pipe", index=False)
    return table_str

def markdown_index_table(header, contents):
    df = pd.DataFrame(contents, columns=header)
    table_str = df.to_markdown(headers=header, tablefmt="pipe")
    return table_str

def dfloader_format_table(header, contents):
    data = {col: data_col for col, data_col in zip(header, zip(*contents))}

    df = pd.DataFrame(data)

    # 构建DataFrame字符串表示
    df_str = "pd.DataFrame({\n"
    for col in df.columns:
        df_str += f"    '{col}': {df[col].tolist()},\n"
    df_str += "},\n"
    df_str += f"index={df.index.tolist()})"
    return df_str

def json_table(header, contents):
    df = pd.DataFrame(contents, columns=header)
    table_str = df.to_json(orient='index')
    return table_str
    
def matrix_table(header, contents):
    table_str = ""
    header = [""] + header
    output_list = []
    table_str += "[\n"
    table_str += "  " + str(header) + "\n"
    for i, row in enumerate(contents):
        table_str += "  " + str([i] + row) + "\n"
    table_str += "]"
    return table_str

def html_table(header, contents):
    df = pd.DataFrame(contents, columns=header)
    table_str = df.to_html()
    return table_str

def csv_table(header, contents):
    df = pd.DataFrame(contents, columns=header)
    table_str = df.to_csv(index=True)
    return table_str

def tab_table(header, contents):
    df = pd.DataFrame(contents, columns=header)
    table_str = str(df)
    return table_str
    
def flatten_table(header, contents):
    header_string = f'The table have {len(header)} columns: '
    header_string += " | ".join(header) + '\n'
    value_string = ""
    for i, row in enumerate(contents):
        value_string += "row " + str(i+1) + " : "
        row_cell_values = [str(cell_value).lower() if isinstance(cell_value, str) else str(cell_value) if isinstance(cell_value, int) else '' for cell_value in row]
        row_value_string = ""
        for j, value in enumerate(row_cell_values):
            row_value_string += f"{header[j]} is {value}. "
        value_string += row_value_string + '\n'
    output_string = header_string + value_string

    return output_string

def tapex_table(header, contents):
    header_string = f"header: "
    header_string += " | ".join(header) + '\n'
    value_string = ""
    for i, row in enumerate(contents):
        value_string += "row " + str(i+1) + " : "
        row_cell_values = [str(cell_value).lower() if isinstance(cell_value, str) else str(cell_value) if isinstance(cell_value, int) else '' for cell_value in row]
        row_value_string = " | ".join(row_cell_values) + '\n'
        value_string += row_value_string
    output_string = header_string + value_string
    return output_string
    




def get_table_str(header, contents, format):
    if format == 'markdown':
        return markdown_table(header, contents)
    elif format == 'markdown_index':
        return markdown_index_table(header, contents)
    elif format == 'dfloader':
        return dfloader_format_table(header, contents)
    elif format == 'json':
        return json_table(header, contents)
    elif format == 'matrix':
        return matrix_table(header, contents)
    elif format == 'html':
        return html_table(header, contents)
    elif format == 'csv':
        return csv_table(header, contents)
    elif format == 'tab':
        return tab_table(header, contents)
    elif format == 'flatten':
        return flatten_table(header, contents)
    elif format == 'tapex':
        return tapex_table(header, contents)
