from .table_utils import read_table
from .table_utils import find_element_position
from tqdm import tqdm
import random
import pandas as pd
import sqlite3
from .custom_template_parser import generate_process, generate_multiturn
from .custom_template_parser import cover_column, cover_row, calculate_depth, calculate_times
from .table_utils import execute_sql




def control_sql(header, contents, sql, answer, process, details, sql_config, table_path):
    row_num = len(contents)
    col_num = len(contents[0])
    
    # 如果没有控制条件，直接返回True
    if not sql_config:
        return True
    # 对keyword做操作
    sql_keywords = process.keys()
    exclude_keywords = [key for key, value in sql_config.items() if value is False]
    intersection = list(set(sql_keywords) & set(exclude_keywords))
    if len(intersection) != 0:
        return False


    # length 筛选
    if sql_config['length_setting']['is_available']:
        sql_length = len(sql.split(' '))
        gold_length = []
        if len(sql_config['length_setting']['value']) != 0:
            gold_length = sql_config['length_setting']['value']
        else:
            gold_length = list(range(sql_config['length_setting']['min'], sql_config['length_setting']['max']+1))
        if sql_length not in gold_length:
            return False
    
    # column_ratio
    if sql_config['column_ratio']['is_available']:
        select_col_num = cover_column(contents, process)
        if len(sql_config['column_ratio']['value']) != 0:
            if select_col_num not in sql_config['column_ratio']['value']:
                return False
        else:
            if not (sql_config['column_ratio']['min'] <= select_col_num / col_num < sql_config['column_ratio']['max']):
                return False

    # select_row_ratio
    if sql_config['select_row_ratio']['is_available']:
        select_row_num = cover_row(contents, process)
        if len(sql_config['select_row_ratio']['value']) != 0:
            if select_row_num not in sql_config['select_row_ratio']['value']:
                return False
        else:
            if not (sql_config['select_row_ratio']['min'] < select_row_num / row_num <= sql_config['select_row_ratio']['max']):
                return False
    
    # calculate times
    if sql_config['calculate_times']['is_available']:
        time = calculate_times(process, sql)
        if time not in sql_config['calculate_times']['value']:
            return False

    def replace_chars(s, start_index, end_index, origin, new):
        return s[:start_index] + s[start_index:end_index].replace(origin, new) + s[end_index:]
    
    # answer location
    if sql_config['answer_location']['is_available']:
        row_index, col_index = 0, 0
        if details['select'][1] in ['count', 'min', 'max', 'sum', 'avg']:
            agg = details['select'][1]
            non_agg_sql = replace_chars(sql, 0, 30, f'{agg} (', '')
            non_agg_sql = replace_chars(non_agg_sql, 0, 30, f')', '')
            try:
                output = execute_sql(table_path, non_agg_sql)
            except:
                import pdb; pdb.set_trace()
            row_index, col_index = find_element_position(contents, output[0][0])
        else:
            row_index, col_index = find_element_position(contents, answer)

        if len(sql_config['answer_location']['row_value']) != 0:
            if row_index not in sql_config['answer_location']['row_value']:
                return False
        if len(sql_config['answer_location']['column_value']) != 0:
            if isinstance(col_index, list):
                for index in col_index:
                    if index not in sql_config['answer_location']['column_value']:
                        return False
            else:
                if col_index not in sql_config['answer_location']['column_value']:
                    return False
        else:
            if not ( sql_config['answer_location']['min'] < row_index / row_num <= sql_config['answer_location']['max']):
                return False
        
    # if len(set(sql.split(' ')).intersection(set(sql_config['include']))) == 0:
    #     return False
    
    if any(item in sql for item in sql_config['exclude']):
        return False
    
    return True

def template_queries(sql_templates, num_queries, table_path, sql_config, multiple, data_mode='ft'):
    header, contents, types = read_table(table_path)

    queries = []        
    
    for _ in tqdm(range(num_queries*multiple)):
        query = random.choice(sql_templates)

        text_col = [col for col, col_type in zip(header, types) if col_type in ['TEXT', 'DATE']]
        int_col = [col for col, col_type in zip(header, types) if col_type in ['INT']]
        # nn = random.sample(text_col, k=2)

        op = ['>', '<', '=']
        # col = int_col + text_col
        query = ' '.join(query)
        
        if '<text_col1> <op1> <text_1>' in query:
            query.replace('<op1>', '=')
        if '<text_col2> <op2> <text_2>' in query:
            query.replace('<op2>', '=')
        if '<text_col3> <op3> <text_3>' in query:
            query.replace('<op3>', '=')
        if '<text_col4> <op4> <text_4>' in query:
            query.replace('<op3>', '=')

        if int_col != []:
            int_col1 = random.choice(int_col)
            if len(int_col) > 1:
                int_col.remove(int_col1)
            int_col2 = random.choice(int_col)
            if len(int_col) > 1:
                int_col.remove(int_col2)
            int_col3 = random.choice(int_col)
        else:
            int_col1, int_col2, int_col3 = "", "", ""
        if sql_config["answer_location"]["is_available"]:
            text_col1 = text_col[sql_config["answer_location"]["column_value"][0]]
        else:
            text_col1 = random.choice(text_col)
        if len(text_col) > 1:
            text_col.remove(text_col1)
        text_col2 = random.choice(text_col)
        if len(text_col) > 1:
            text_col.remove(text_col2)
        text_col3 = random.choice(text_col)
        if len(text_col) > 1:
            text_col.remove(text_col3)
        text_col4 = random.choice(text_col)
        
        num1 = random.choice(['1', '2', '3', '4', '5', '6', '7', '8'])

        
        query = query.replace('<num_1>', num1)
        
        
        if query.count('<op1>') > 2:
            query = replace_nth_occurrence(query, '<op1>', '<op2>', 2)
            query = replace_nth_occurrence(query, '<op1>', '<op3>', 2)
            
        
        if query.count('<op1>') > 1:
            if '<op2>' not in query:
                query = replace_nth_occurrence(query, '<op1>', '<op2>', 2)
            elif '<op3>' not in query:
                query = replace_nth_occurrence(query, '<op1>', '<op3>', 2)
            else:
                query = replace_nth_occurrence(query, '<op1>', '<op4>', 2)
        elif query.count('<op2>') > 1: 
            if '<op1>' not in query:
                query = replace_nth_occurrence(query, '<op2>', '<op1>', 2)
            elif '<op3>' not in query:
                query = replace_nth_occurrence(query, '<op2>', '<op3>', 2)
            else:
                query = replace_nth_occurrence(query, '<op2>', '<op4>', 2)
        

        query = query.replace('<int_col1>', int_col1)
        query = query.replace('<int_col2>', int_col2)
        query = query.replace('<int_col3>', int_col3)
        query = query.replace('<text_col1>', text_col1)
        query = query.replace('<text_col2>', text_col2)          
        query = query.replace('<text_col3>', text_col3) 
        query = query.replace('<text_col4>', text_col4) 
        query = query.replace('<op1>', random.choice(op))
        query = query.replace('<op2>', random.choice(op))
        query = query.replace('<op3>', random.choice(op))
        query = query.replace('<op4>', random.choice(op))

        r_dict = {
            '<int_1>': int_col1,
            '<int_2>': int_col2,
            '<int_3>': int_col3,
            '<text_1>': text_col1,
            '<text_2>': text_col2,
            '<text_3>': text_col3,
            '<text_4>': text_col4
        }
        

        for key in r_dict.keys():
            if key in query:
                count_number = count_substring_occurrences(query, key)
                if sql_config["answer_location"]["is_available"] and len(sql_config["answer_location"]["row_value"]) != 0:
                    row_index = sql_config["answer_location"]["row_value"][0]
                    if count_number > 1:
                        if 'int' in key:
                            query = query.replace(key, str(contents[row_index][header.index(r_dict[key])]))
                        else:
                            query = query.replace(key, f"'{str(contents[row_index][header.index(r_dict[key])])}'")
                    else:
                        if 'int' in key:
                            replacement_list = [str(contents[row_index][header.index(r_dict[key])]) for _ in range(count_number)]
                        else:
                            replacement_list = [f"'{str(contents[row_index][header.index(r_dict[key])])}'" for _ in range(count_number)]
                        query = replace_substring_occurrences(query, key, replacement_list)
                else:
                    text_col1 = random.choice(text_col)
                    if count_number > 1:
                        if 'int' in key:
                            query = query.replace(key, str(random.choice(contents)[header.index(r_dict[key])]))
                        else:
                            query = query.replace(key, f"'{str(random.choice(contents)[header.index(r_dict[key])])}'")
                    else:
                        if 'int' in key:
                            replacement_list = [str(random.choice(contents)[header.index(r_dict[key])]) for _ in range(count_number)]
                        else:
                            replacement_list = [f"'{str(random.choice(contents)[header.index(r_dict[key])])}'" for _ in range(count_number)]
                        query = replace_substring_occurrences(query, key, replacement_list)

        queries.append(query)
        
        
        
    conn = sqlite3.connect(table_path)
    # 创建游标对象
    cursor = conn.cursor()

    count = 0
    new_sql = []
    new_answer = []

    for sql in queries:
        try:
            cursor.execute(sql)
        except:
            continue
        answer = cursor.fetchall()
        if answer is None:
            continue
        answer_cells_number = 1
        if sql_config["answer_cells_number"]:
            answer_cells_number = sql_config["answer_cells_number"]
        if answer_cells_number == 1:      
            if len(answer) == answer_cells_number:
                count += 1
                new_sql.append(sql)
                new_answer.append(str(answer[0][0]))
        else:
            if len(answer) == answer_cells_number:
                count += 1
                new_sql.append(sql)
                new_answer.append([item[0] for item in answer])


    if data_mode == 'ft':
        new_data = []
        for i in range(len(new_sql)):
            new_dict = {}

            input_format = "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable: {}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\n\nSQL:{}\nAnswer:"
            df = pd.DataFrame(contents, columns=header)
            table_str = df.to_markdown(headers=header, tablefmt="pipe")
            inputs = input_format.format(table_str, new_sql[i])
            outputs = new_answer[i]
            
            new_dict['input'] = inputs
            new_dict['output'] = outputs
            new_dict['sql'] = new_sql[i]
            new_dict['answer'] = new_answer[i]
            new_data.append(new_dict)
            
        
        final_data = []
        sql_set = set()
        for item in new_data:
            if item['sql'] in sql_set:
                continue
            else:
                final_data.append(item)
                sql_set.add(item['sql'])
        

        final_data = final_data[:num_queries]   
        print(len(new_sql), len(final_data))    
        return final_data
    elif data_mode == 'eval':
        new_data = []
        for i in range(len(new_sql)):
            new_dict = {}
            new_dict['sql'] = new_sql[i]
            new_dict['answer'] = new_answer[i]
            process, details = generate_process(header, contents, new_sql[i], table_path)
            if not control_sql(header, contents, new_sql[i], new_answer[i], process, details, sql_config, table_path):
                continue

            if sql_config["multi_test"]:
                select_where = new_sql[i].split('where')[0] + 'where'
                condition1 = new_sql[i].split('where')[1].split('and')[0]
                condition2 = new_sql[i].split('where')[1].split('and')[1]
                sql1 = select_where + condition1
                sql2 = select_where + condition2
              
                try:
                    cursor.execute(sql1)
                except:
                    continue
                answer1 = cursor.fetchall()
                if len(answer1) < 2 or len(answer1) > 5:
                    continue
                try:
                    cursor.execute(sql2)
                except:
                    continue
                answer2 = cursor.fetchall()
                if len(answer2) < 2 or len(answer2) > 5:
                    continue
                new_dict["sql1"] = sql1
                new_dict["sql2"] = sql2
                new_dict["answer1"] = answer1
                new_dict["answer2"] = answer2
            
            multiturn = generate_multiturn(details, header)
            new_dict['multiturn'] = ''.join(multiturn)
            new_data.append(new_dict)
            
        
        final_data = []
        sql_set = set()
        for item in new_data:
            if item['sql'] in sql_set:
                continue
            else:
                final_data.append(item)
                sql_set.add(item['sql'])
                

        n_shot = sql_config['n_shot']
        examples = []

        try:
            for i in range(n_shot):
                examples.append({'sql':final_data[i]['sql'], 'answer':final_data[i]['answer'], 'multiturn':final_data[i]['multiturn']})
        except:
            print(len(new_sql), len(new_data), len(final_data)) 
            return [] 
        
        output_data = []
        for data in final_data[n_shot:]:
            new_dict = {}
            new_dict['header'] = header
            new_dict['contents'] = contents
            data_examples = [{'sql': data['sql'], 'answer':data['answer'], 'multiturn':data['multiturn']}]
            new_dict['examples'] = data_examples + examples
            if sql_config["multi_test"]:
                new_dict['subsql'] = {'sql1':data['sql1'], 'answer1':data['answer1'], 'sql2':data['sql2'], 'answer2':data['answer2']}
            output_data.append(new_dict)

        output_data = output_data[:num_queries]   
        print(len(new_sql), len(new_data), len(output_data))    
        return output_data




def replace_nth_occurrence(string, substring, replacement, n):
    count = 0
    start = 0

    while count < n:
        index = string.find(substring, start)
        if index == -1:
            break
        count += 1
        start = index + len(substring)

    if count < n:
        raise ValueError(f"The substring '{substring}' does not occur {n} times in the string.")

    return string[:index] + replacement + string[index + len(substring):]

def replace_substring_occurrences(string, substring, replacement_list):
    count = string.count(substring)
    if count != len(replacement_list):
        raise ValueError("The count of substring does not match the length of replacement_list.")

    for replacement in replacement_list:
        string = string.replace(substring, replacement, 1)

    return string

def count_substring_occurrences(string, substring):
    count = 0
    start = 0

    while True:
        index = string.find(substring, start)
        if index == -1:
            break
        count += 1
        start = index + len(substring)

    return count

def replace_nth_occurrence(string, substring, replacement, n):
    count = 0
    start = 0

    while count < n:
        index = string.find(substring, start)
        if index == -1:
            break
        count += 1
        start = index + len(substring)

    if count < n:
        raise ValueError(f"The substring '{substring}' does not occur {n} times in the string.")

    return string[:index] + replacement + string[index + len(substring):]
