from .table_utils import read_table
from .table_utils import find_element_position
from tqdm import tqdm
import random
import pandas as pd
import sqlite3
from .parser import filter_times, calculate_times
from .value_utils import random_with_weight, random_dict_key, random_dict_value, remove_double_spaces, random_dict_key_value, extract_subsql_position, merge_dicts
from .table_utils import execute_sql, transform_output_to_tablestr, generate_intermedium_table, transform_output_to_string

code_chinese = {'>': '大于', '<': '小于', '=': '等于', 'count':'数量', 'max': '最大值', 'min': '最小值', 'sum': '和', 'avg':'平均值', '+':'之和', '-':'之差', '*':'之积', '/':'之商'}



def control_sql_general(header, contents, sql, answer, col_dict, select_rows_list, sql_config):
    # if there are no control conditions, return True.
    if not sql_config:
        return True
    row_num = len(contents)
    col_num = len(contents[0])
    
    # Control the length of the answer.
    if isinstance(sql_config['answer_cells_number'], int):
        if len(answer) != sql_config['answer_cells_number']:
            return False
    else:
        if len(answer) == 0:
            return False

    # keyword
    exclude_keywords = [key for key, value in sql_config['keywords_setting'].items() if value is False]

    for keyword in exclude_keywords:
        if keyword in sql:
            return False
    
    # length
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
    col_list = list(set([value for values in col_dict.values() for value in values]))
    if sql_config['column_ratio']['is_available']:
        select_col_num = len(col_list)
        if len(sql_config['column_ratio']['value']) != 0:
            if select_col_num not in sql_config['column_ratio']['value']:
                return False
        else:
            if not (sql_config['column_ratio']['min'] <= select_col_num / col_num <= sql_config['column_ratio']['max']):
                return False

    # select_row_ratio
    if sql_config['select_row_ratio']['is_available']:
        select_row_num = len(select_rows_list)
        if len(sql_config['select_row_ratio']['value']) != 0:
            if select_row_num not in sql_config['select_row_ratio']['value']:
                return False
        else:
            if not (sql_config['select_row_ratio']['min'] <= select_row_num / row_num <= sql_config['select_row_ratio']['max']):
                return False

    # Filter times
    if sql_config['filter_times']['is_available']:
        depth = filter_times(sql)
        if depth not in sql_config['filter_times']['value']:
            return False
            
            
    # calculate times
    if sql_config['calculate_times']['is_available']:
        time = calculate_times(sql)
        if time not in sql_config['calculate_times']['value']:
            return False

    # Answer Location
    if sql_config['answer_location']['is_available']:
        row_index, col_index = find_element_position(contents, answer)
        if len(sql_config['answer_location']['row_value']) != 0:
            if row_index not in sql_config['answer_location']['row_value']:
                return False
            if col_index not in sql_config['answer_location']['column_value']:
                return False
        else:
            if not ( sql_config['answer_location']['min'] < row_index / row_num <= sql_config['answer_location']['max']):
                return False

    # if any(item not in sql for item in sql_config['include']):
    #     return False
    
    if any(item in sql for item in sql_config['exclude']):
        return False
    
    return True



   
# Return ( select_part, select_cols, select_instruction, select_agg, select_process)
def select_condition(text_cols, int_cols):
    select_part = ""
    select_cols = []
    select_instruction = ""
    select_agg = ""
    select_process = ('','')
    
    # Set "+, -, *, /" as the value for select_col.
    if random_with_weight([True, False], [0.05, 0.95]):
        select_col1, select_col2 = random.choices(int_cols, k=2)
        cal_ops = ['+', '-', '*', '/']
        cal_op = random_with_weight(cal_ops, [0.5,0.3,0.1,0.1]) 
        if random_with_weight([True, False], [0.8, 0.2]):
            select_part = f"{select_col1} {cal_op} {select_col2}"
            select_cols = [select_col1, select_col2]
            select_instruction = f"选择，并计算，已筛选行中，两列 {select_col1} 和 {select_col2}{code_chinese[cal_op]}。 "
        else:
            agg = random.choice(['min', 'max', 'count', 'sum', 'avg'])
            select_part = f"{agg} ( {select_col1} {cal_op} {select_col2} )"
            select_cols = [select_col1, select_col2]
            select_instruction = f"选择，并计算，已筛选行中，两列 {select_col1} 和 {select_col2}{code_chinese[cal_op]}，并求{code_chinese[agg]}。 "
            select_agg = agg
    # Common select_col
    else:    
        select_col = random.choice(text_cols + int_cols)
        # if no agg, directly return this row
        if random_with_weight([True, False], [0.8, 0.2]):
            select_part = select_col
            select_cols = [select_col]
            select_instruction = f"选择，已筛选行中的 {select_col} 列"
            select_process = (select_col, "")
        else:
            if select_col in int_cols:
                agg = random.choice(['min', 'max', 'count', 'sum', 'avg'])
                select_part = f"{agg} ( {select_col} )"
                select_cols = [select_col]
                select_instruction = f"选择，已筛选行中的{select_col} 列，并求{code_chinese[agg]}"
                select_agg = agg
                select_process = (select_col, select_agg)
            elif select_col in text_cols:
                select_part = f"count ( {select_col} )"
                select_cols = [select_col]
                select_instruction = f"选择，已筛选行中的{select_col} 列的数量"
                select_agg = 'count'
                select_process = (select_col, select_agg)
    return select_part, select_cols, select_instruction, select_agg, select_process
    
def where_condition(header, contents, text_cols, int_cols):
    # random where number
    where_num = random_with_weight([1, 2], [0.7, 0.3])
    where_string_output = ""
    where_cols = []
    where_instruction = "请按需要满足的列条件筛选行： "
    where_string = []
    where_process = []
    for i in range(where_num):
        select_col = random.choice(text_cols + int_cols)
        if select_col in text_cols:
            text_ops = ['=', 'like', 'in']
            op = random_with_weight(text_ops, [0.6, 0.2, 0.2])
            if op == '=':
                value = f"'{random.choice(contents)[header.index(select_col)]}'"
                op_value = f"{op} {value}"
                # condition
                where_process.append( (select_col, op, value)  )
                # to NL
                where_string.append(f"列 {select_col} 的值是 {value}.")
            elif op == 'like':
                value = random.choice(contents)[header.index(select_col)]
                like_value = value[:3]
                op_value = f"{op} '{like_value}%'"
                # condition
                where_process.append( (select_col, op, like_value)  )
                # to NL
                where_string.append(f"列 {select_col} 的值模糊匹配'{like_value}%'.")
            elif op == 'in':
                in_number = random_with_weight([2,3],[0.6,0.4])
                value = [row[header.index(select_col)] for row in random.choices(contents, k=3)]
                if in_number == 2:
                    in_value = f"( '{value[0]}' , '{value[1]}' )"
                    # condition
                    where_process.append( (select_col, op, [value[0], value[1]])  )
                    # to NL
                    where_string.append(f"列 {select_col} 的值是 '{value[0]}' 或 '{value[1]}'.")
                elif in_number == 3:
                    in_value = f"( '{value[0]}' , '{value[1]}' , '{value[2]}' )"
                    # condition
                    where_process.append( (select_col, op, [value[0], value[1], value[2]])  )
                    # to NL
                    where_string.append(f"列 {select_col} 的值是 '{value[0]}' 或 '{value[1]}' 或 '{value[2]}'.")
                op_value = f"{op} {in_value}"
        elif select_col in int_cols:
            op = random.choice(['>', '<', '='])
            value = f"{random.choice(contents)[header.index(select_col)]}"
            op_value = f"{op} {value}"
            # condition
            where_process.append( (select_col, op, value)  )
            # to NL
            where_string.append(f"列 {select_col} 的值 {code_chinese[op]} {value}.")
            
        if i == 0:
            where_string_output += f"where {select_col} {op_value}"
        elif i == 1:
            where_string_output += f" and {select_col} {op_value}"
        where_cols.append(select_col)
    where_instruction += " ".join(where_string)
    return where_string_output, where_cols, where_instruction, where_process

def order_condition(text_cols, int_cols, mode='single'):
    order_instruction_temp = "将已选择的值按 {} {}排序，并且选择最{}的值。"
    order = random.choice(['asc', 'desc'])
    order_process = []

    if mode == 'group':
        select_col = random.choice(text_cols + int_cols)
        is_distinct_select_col = random.choice([f'count ( {select_col} )', f'count ( distinct {select_col} )'])
        if order == 'asc':
            if 'distinct' in is_distinct_select_col:
                order_instruction = order_instruction_temp.format(f"{select_col} 列未重复的数量", '升序', '小')
            else:
                order_instruction = order_instruction_temp.format(f"{select_col} 列的数量", "升序", '小')
            order_process.append(   (select_col, 0)   )
        else:
            if 'distinct' in is_distinct_select_col:
                order_instruction = order_instruction_temp.format(f"{select_col} 列未重复的数量", "降序", '大')
            else:
                order_instruction = order_instruction_temp.format(f"{select_col} 列的数量", "升序", '大')
            order_process.append(   (select_col, 1)   )
        return f"order by {is_distinct_select_col} {order} limit 1", [select_col], order_instruction, order_process
    else:
        select_col = random.choice(int_cols)
        if order == 'asc':
            order_instruction = order_instruction_temp.format('升序', select_col, '小')
            order_process.append(   (select_col, 0)   )
        else:
            order_instruction = order_instruction_temp.format('降序', select_col, '大')
            order_process.append(   (select_col, 1)   )
        return f"order by {select_col} {order} limit 1", [select_col], order_instruction, order_process
    
    
def having_condition(header, contents, text_cols, int_cols):
    having_instruction = "然后根据以下条件筛选某些组："
    
    condition_num = random_with_weight([1, 2], [0.7, 0.3])
    having_conditions = []
    having_conditions_instructions = []
    having_cols = []
    having_process = []
    
    for i in range(condition_num):
        # if aggregation
        if random_with_weight([True, False], [0.7, 0.3]):
            select_col = random.choice(text_cols + int_cols)
            num1 = random.choice([1,2,3,4,5,6])
            op = random.choice(['>', '<', '='])
            if select_col in text_cols:
                is_distinct_select_col = random.choice([f'count ( {select_col} )', f'count ( distinct {select_col} )'])
                having_conditions.append(f"{is_distinct_select_col} {op} {str(num1)}")
                having_cols.append(select_col)
                if 'distinct' in is_distinct_select_col:
                    having_conditions_instructions.append(f"{select_col} 列未重复的数量 {code_chinese[op]} {str(num1)}")
                else:
                    having_conditions_instructions.append(f"{select_col} 列的数量 {code_chinese[op]} {str(num1)}.")
                having_process.append( (select_col, op, num1)  )
            else:
                agg = random.choice(['count', 'max', 'min', 'sum', 'avg'])
                if agg == 'count':
                    is_distinct_select_col = random.choice([f'count ( {select_col} )', f'count ( distinct {select_col} )'])
                    having_conditions.append(f"{is_distinct_select_col} {op} {str(num1)}")
                    having_cols.append(select_col)
                    if 'distinct' in is_distinct_select_col:
                        having_conditions_instructions.append(f"{select_col} 列未重复的数量 {code_chinese[op]} {str(num1)}.")
                    else:
                        having_conditions_instructions.append(f"{select_col} 列的数量 {code_chinese[op]} {str(num1)}.")
                    having_process.append( (select_col, op, num1, agg)  )
                else:
                    agg_select_col = f"{agg} ( {select_col} )"
                    value = f"{random.choice(contents)[header.index(select_col)]}"
                    having_conditions.append(f"{agg_select_col} {op} {str(value)}")
                    having_cols.append(select_col)
                    having_conditions_instructions.append(f"{select_col} 列的{code_chinese[agg]} {code_chinese[op]} {str(num1)}.")
                    having_process.append( (select_col, op, num1, agg)  )
        # no aggregation
        else:
            select_col = random.choice(int_cols)
            op = random.choice(['>', '<'])
            value = f"{random.choice(contents)[header.index(select_col)]}"
            having_conditions.append(f"{select_col} {op} {str(value)}")
            having_cols.append(select_col)
            having_conditions_instructions.append(f"{select_col} 列 {code_chinese[op]} {str(value)}.")
            having_process.append( (select_col, op, value)  )
    having_conditions_output = ""
    if len(having_conditions) == 1:
        having_conditions_output = f"having {having_conditions[0]}"
    elif len(having_conditions) == 2:
        having_conditions_output = f"having {having_conditions[0]} and {having_conditions[1]}"
        
    having_instruction += " ".join(having_conditions_instructions)
    return having_conditions_output, having_cols, having_instruction, having_process
    

    
        
def group_condition(header, contents, text_cols, int_cols):
    group_col = random.choice(text_cols)
    
    mode = random.choice(['having', 'order'])
    group_instruction = f"然后根据剩余行中{group_col}的值对行进行分组."
    group_process = [group_col]
    return f"group by {group_col}", [group_col], group_instruction, group_process
        
    
     
def general_queries(sql_templates, num_queries, table_path, sql_config, multiple, data_mode='ft'):
    header, contents, types = read_table(table_path)
    text_cols = [col for col, col_type in zip(header, types) if col_type in ['TEXT', 'DATE']]
    int_cols = [col for col, col_type in zip(header, types) if col_type in ['INT']]
    
    new_templates = {}
    
    s_i, d_i, t_i = 1, 1, 1
    
    for template in sql_templates:
        if template.count("(") == 0:
            new_templates.update({f"s{s_i}":template})
            s_i += 1
        elif template.count("(") == 1:
            new_templates.update({f"d{d_i}":template})
            d_i += 1
        elif template.count("(") == 2:
            new_templates.update({f"t{t_i}":template})
            t_i += 1

    nest_filter = sql_config['nest']
    for nest in nest_filter:
        if nest == 1:
            new_templates.update({key: value for key, value in sql_templates.items() if key.startswith('s')})
        elif nest == 2:
            new_templates.update({key: value for key, value in sql_templates.items() if key.startswith('d')})
        elif nest == 3:
            new_templates.update({key: value for key, value in sql_templates.items() if key.startswith('t')})
            
    def get_select_rows(query):
        new_query = query.replace('<order_condition>', '')
        aggregate_word = ['min', 'max', 'count', 'sum', 'avg']
        
        try:
            front_str, back_str =  new_query.split('my_table')
        except:
            import pdb; pdb.set_trace()
        for agg in aggregate_word:
            front_str = front_str.replace(f"{agg} (", "")
        front_str = front_str.replace(")", "")
        new_query = front_str + 'my_table' + back_str
        
        conn = sqlite3.connect(table_path)
        cursor = conn.cursor()
        try:
            cursor.execute(new_query)
        except:
            import pdb; pdb.set_trace()
        answer = cursor.fetchall()

        return answer
    
    def generate_sql_cot(query, cols, change_string, change_process, change_instruction):
        df = pd.DataFrame(contents, columns=header)
        origin_table = df.to_markdown(headers=header, tablefmt="pipe")
        instrutions = []
        intermediems = []
        if change_string["where"] != "":
            temp_sql = "select * from my_table " + change_string["where"]
            output = execute_sql(table_path, temp_sql)
            new_contents = [list(item) for item in output]
            intermediem = transform_output_to_tablestr(header, new_contents)
            instrutions.append(change_instruction["where"])
            intermediems.append(intermediem)
        if change_string["group"] != "":
            temp_sql = "select * from my_table " + change_string["where"] + " "+ change_string["group"]
            group_col = cols['group_col'][0]
            group_col_index = header.index(group_col)
            output = execute_sql(table_path, temp_sql)
            result = [item[group_col_index] for item in output]
            intermediem = ",".join(result)
            instrutions.append(change_instruction["group"])
            intermediems.append(f"Divided rows into {len(output)} groups based on column {group_col}: "+ intermediem)
        if change_string["having"] != "":
            temp_sql = "select * from my_table " + change_string["where"] + " "+change_string["group"] + " "+change_string['having']
            output = execute_sql(table_path, temp_sql)
            new_contents = [list(item) for item in output]
            intermediem = transform_output_to_tablestr(header, new_contents)
            instrutions.append(change_instruction["having"])
            intermediems.append(intermediem)
        if change_string["select"] != "":
            if change_process['select'][1] == '':
                temp_sql = f"select {change_string['select']} from my_table " + " "+change_string["where"] + " "+change_string["group"] + " "+change_string['having']
                output = execute_sql(table_path, temp_sql)
                result = transform_output_to_string(output)
                instrutions.append(change_instruction["select"])
                intermediems.append(result)
            else:
                agg = change_process['select'][1]
                select_string = change_string['select'].replace(f'{agg} (','').replace(')','')
                temp_sql1 = f"select {select_string} from my_table " + " "+change_string["where"] + " "+change_string["group"] + " "+change_string['having']
                output1 = execute_sql(table_path, temp_sql1)
                result1 = transform_output_to_string(output1)
                instrutions.append(f"从已筛选的 {cols['select_col'][0]} 列中选择单元格")
                intermediems.append(result1)
                
                temp_sql2 = f"select {change_string['select']} from my_table " + " "+change_string["where"] + " "+change_string["group"] + " "+change_string['having']
                output2 = execute_sql(table_path, temp_sql2)
                result2 = transform_output_to_string(output2)
                instrutions.append(f"计算 {code_chinese[agg]} 单元格.")
                intermediems.append(result2)
        if change_string["order"] != "":
            output = execute_sql(table_path, query)
            result = transform_output_to_string(output)
            instrutions.append(change_instruction["order"])
            intermediems.append(result)
            
        try:
            sql_cot = f"You need to execute {len(instrutions)} steps.\n"
            for i in range(len(instrutions)-1):
                sql_cot += f"Step {i}: {instrutions[i]}\n"
                sql_cot += f"Intermediate results {i}:\n{intermediems[i]}\n"
            sql_cot += f"Step {len(instrutions)-1}: {instrutions[len(instrutions)-1]}\n"
            sql_cot += f"Answer: {intermediems[len(instrutions)-1]}"
        except:
            import pdb; pdb.set_trace()

        return sql_cot

    # Return (processed query, selected dict，selected row, instruction, select_agg)
    def single_template_generate(query, template='s'):
        ##### 预定义
        select_col, where_col, order_col, group_col, having_col = [], [], [], [], []
        where_part, group_part, order_part, having_part, select_part = "", "", "", "", ""
        where_process, group_process, order_process, having_process, select_process = [], [], [], [], []
        #####
        select_rows_list = []
        output_cols = []
        instruction = ""
        select_agg = []
        sql_cot = ""
        select_instruction, group_instruction, order_instruction,having_instruction, where_instruction = "","","","",""
        select_agg = None
        if '<where_condition>' in query:
            where_part, where_col, where_instruction, where_process = where_condition(header, contents, text_cols, int_cols)
            query = query.replace('<where_condition>', where_part)
        if '<group_condition>' in query:
            group_part, group_col, group_instruction, group_process = group_condition(header, contents, text_cols, int_cols)
            query = query.replace('<group_condition>', group_part)
            if '<order_condition>' in query:
                order_part, order_col, order_instruction, order_process = order_condition(text_cols, int_cols, mode='group')
                query = query.replace('<order_condition>', order_part)

        if '<having_condition>' in query:
            having_part, having_col, having_instruction, having_process = having_condition(header, contents, text_cols, int_cols)
            query = query.replace('<having_condition>', having_part)
        if '<select_condition>' in query:
            select_part, select_col, select_instruction, select_agg, select_process = select_condition(text_cols, int_cols)
            query = query.replace('<select_condition>', select_part)
            if template == 's':
                select_rows_list = get_select_rows(query)
        if '<order_condition>' in query:
            order_part, order_col, order_instruction, order_process = order_condition(text_cols, int_cols)
            query = query.replace('<order_condition>', order_part)
            
        output_cols = {'select_col':select_col, 'where_col':where_col, 'order_col':order_col, 'group_col':group_col, 'having_col':having_col}
        if template == 's':
            instruction = where_instruction + group_instruction + having_instruction + select_instruction + order_instruction
            change_string = {'select':select_part, 'where':where_part, 'order':order_part, 'having':having_part, 'group':group_part }
            change_process = {'select':select_process, 'where':where_process, 'order':order_process, 'having':having_process, 'group':group_process }
            change_instruction = {'select': select_instruction, 'where': where_instruction, 'order': order_instruction, 'having':having_instruction, 'group': group_instruction}
            sql_cot = generate_sql_cot(query, output_cols, change_string, change_process, change_instruction)
            instruction = where_instruction + "\n" + group_instruction + "\n" + having_instruction + "\n" + select_instruction + "\n" + order_instruction
        return query, output_cols, select_rows_list, instruction, select_agg, sql_cot
    
    def generate_op_and_value(select_col, select_agg):
        if select_col in text_cols:
            op = '='
            value = f"'{random.choice(contents)[header.index(select_col)]}'"
        elif select_col in int_cols:
            op = random.choice(['>', '<', '='])
            value = f"{random.choice(contents)[header.index(select_col)]}"
            if select_agg == 'count':
                value = random.choice([1, 2, 3, 4, 5])
        return f'{op} {value}'
    def generate_col_and_value(select_col):
        if select_col in text_cols:
            op = '='
        elif select_col in int_cols:
            op = random.choice(['>', '<', '='])
        return f'{select_col} {op}'

    queries = []
    for _ in tqdm(range(num_queries*multiple)):
        # s: 1 select，d: 2 select，t: 3 select
        id, query = random_dict_key_value(new_templates)
        if id.startswith('s'):
            query, col_dict, select_rows_list, instruction, select_agg, sql_cot = single_template_generate(query)
        elif id.startswith('d'):
            # Template string that needs to be replaced and the list of available subqueries.
            to_replace_str, templist = extract_subsql_position(query)
            selected_template = random.choice([sql_templates[temp] for temp in templist])
            # Generate subsql
            subsql, subsql_cols_dict, subsql_rows_list, subsql_instruction, subsql_select_agg, sql_cot1 = single_template_generate(selected_template, template='d')
            query = query.replace(to_replace_str, subsql)
            select_col = subsql_cols_dict['select_col'][0]
            if '<op_and_value>' in query:
                query = query.replace('<op_and_value>', generate_op_and_value(select_col, subsql_select_agg))
            if '<col_and_op>' in query:
                query = query.replace('<col_and_op>', generate_col_and_value(select_col))
            query, sql_cols_dict, sql_rows_list, instruction, select_agg, sql_cot2 = single_template_generate(query, template='d')
            col_dict = merge_dicts(subsql_cols_dict, sql_cols_dict)
            select_rows_list = subsql_rows_list + sql_rows_list
            instruction = subsql_instruction + instruction
            
            # select_agg = select_agg + subsql_select_agg
            sql_cot = sql_cot1 + sql_cot2
        elif id.startswith('t'):
            to_replace_str, templist = extract_subsql_position(query)
            selected_template = random.choice([sql_templates[temp] for temp in templist])
            subsql1, subsql_cols_dict1, select_rows_list1, instruction1, select_agg1, sql_cot1 = single_template_generate(selected_template, template='t')
            query = query.replace(to_replace_str, subsql1, 1)
            subsql2, subsql_cols_dict2, select_rows_list2, instruction2, select_agg2, sql_cot2 = single_template_generate(selected_template, template='t')
            query = query.replace(to_replace_str, subsql2, 1)
            if '<op>' in query:
                op = random.choice(['>', '<', '=', '+', '-'])
                query = query.replace('<op>', op)
            col_dict = merge_dicts(subsql_cols_dict1, subsql_cols_dict2)
            select_rows_list = select_rows_list1 + select_rows_list2
            instruction = instruction1 + instruction2
            # select_agg = select_agg1 + select_agg2
            sql_cot = sql_cot1 + sql_cot2
            
        query = remove_double_spaces(query)
        

        

        
            
        conn = sqlite3.connect(table_path)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
        except:
            import pdb; pdb.set_trace()
            continue
        answer = cursor.fetchall()
        if answer is None:
            continue
        
        if control_sql_general(header, contents, query, answer, col_dict, select_rows_list, sql_config):
            if sql_config["answer_cells_number"] == 1:
                answer = str(answer[0][0])
            elif sql_config["answer_cells_number"] > 1:
                answer = [str(item[0]) for item in answer]
            output_dict = {}
            output_dict["sql"] = query
            output_dict["answer"] = answer
            if sql_config["output_config"]["process"]:
                output_dict["col_dict"] = col_dict
                output_dict["select_rows_list"] = select_rows_list
            if sql_config["output_config"]["multistep"]:
                output_dict["multistep"] = instruction
            if sql_config["output_config"]["cot"]:
                output_dict["sql_cot"] = sql_cot
            queries.append(output_dict)
    
    
    
    # Remove duplicate SQL
    final_data = [] 
    sql_set = set()   
    for query in queries:
        if query['sql'] in sql_set:
            continue
        else:
            final_data.append(query)
            sql_set.add(query['sql'])

    # Output data
    if data_mode == 'eval' and len(final_data) <= sql_config['n_shot']:
        print(f"Sample:{num_queries*multiple}, Generate:{len(queries)}, No-repeat:{len(final_data)}, Final:0") 
        return []
    elif data_mode == 'ft' and len(final_data) == 0:
        print(f"Sample:{num_queries*multiple}, Generate:{len(queries)}, No-repeat:{len(final_data)}, Final:0") 
        return []
        
    output_data = []
    if data_mode == 'eval':
        n_shot = sql_config['n_shot']
        examples = []
        for i in range(n_shot):
            new_dict = final_data[i]
            examples.append(new_dict)

        for data in final_data[n_shot:]:
            new_dict = {}
            new_dict['header'] = header
            new_dict['contents'] = contents
            data_examples = [data]
            new_dict['examples'] = data_examples + examples
            output_data.append(new_dict)      
                
    elif data_mode == 'ft':
        for data in final_data:
            new_dict = {}
            input_format = "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable: {}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\n\nSQL:{}\nAnswer:"
            df = pd.DataFrame(contents, columns=header)
            table_str = df.to_markdown(headers=header, tablefmt="pipe")
            inputs = input_format.format(table_str, data['sql'])
            outputs = data['answer']
            new_dict['input'] = inputs
            new_dict['output'] = outputs
            new_dict['sql'] = data['sql']
            new_dict['answer'] = data['answer']
            output_data.append(new_dict)
            
    output_data = output_data[:num_queries]  
    print(f"Sample:{num_queries*multiple}, Generate:{len(queries)}, No-repeat:{len(final_data)}, Final:{len(output_data)}") 
    return output_data