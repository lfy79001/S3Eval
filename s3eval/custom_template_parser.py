import re
from .table_utils import execute_sql

def generate_multiturn(details, header):
    def find_column(seq):
        if "*" in seq:
            return ["*"]
        else:
            output = []
            for h in header:
                if h in seq:
                    output.append(h)
            return output
    multiturn = []
    
    if 'where' in details.keys():
        where_sql = details['where'].replace('where', '')    
        multiturn.append(f"Filter the row according to the condition, the cells of the specified column in the row need to meet:{where_sql}.")
    if 'group by' in details.keys():
        group_sql = details['group by']
        multiturn.append(f"Then group all rows by the value of the '{group_sql}' in each row.")
    if 'having' in details.keys():
        group_sql = details['having'].replace('having', '')
        aggregate_dict = {'sum':"the sum of '{}'", 'count':"the number '{}'",
                        'min':"the smallest of '{}'",
                        'max':"the largest of '{}'",
                        'avg':"the average of '{}'"}

        condition = ""
        if 'count ( * )' in group_sql:
            index = group_sql.find('count ( * )')
            result = group_sql[index + len('count ( * )'):]
            condition += f"Number of rows in group {result}"
        else:
            if any(agg in group_sql for agg in aggregate_dict.keys()):
                agg = 0
                for item in aggregate_dict.keys():
                    if item in group_sql:
                       agg = item; break
                column = find_column(group_sql)[0]
                out = aggregate_dict[agg].format(column)
                eq_and_number = ' '.join(group_sql.split(' ')[-2:])
                condition += f"The rows in the group satisfy {out} {eq_and_number}"
            else:
                condition += f"The rows in the group satisfy {group_sql}"
        
        multiturn.append(f"The filtering is then done by group, the filtering conditions are: {condition}.")
    
    select_sql = details['select']
    select_column = select_sql[0][0]
    if 'select' in details.keys():
        select_agg = select_sql[1]
        aggregate_dict = {'sum':'calculate their sum', 'count':"count them",
                        'min':'find the smallest of them',
                        'max':'find the largest of them',
                        'avg':'calculate their average'}
        if select_column == '*':
            if select_agg is None:
                multiturn.append(f"The final answer is the number of filtered rows.")
            else:
                multiturn.append(f"Select all column of filtered rows and then {aggregate_dict[select_agg]} to get the final answer.")
        else:
            if select_agg is None:
                multiturn.append(f"Select cell of '{select_column}' column in filtered rows.")
            else:
                multiturn.append(f"Select cell of '{select_column}' column in filtered rows and then {aggregate_dict[select_agg]}.")
    
    if 'order by' in details.keys():
        
        # if len(details['order by'][0][0])>1:
        #     import pdb; pdb.set_trace()
        order_type = {'asc': ['ascending', 'smallest'],
                      'desc': ['descending', 'largest']}
        
        shunxu = details['order by'][1]
        agg = details['order by'][0][1]
        order_column = details['order by'][0][0][0]
        if len(details['order by'][0][0]) > 1:
            order_column = f"the difference of {details['order by'][0][0][0]} and {details['order by'][0][0][1]}"
        if agg is None:
            multiturn.append(f"Finally, these cells are sorted in {order_type[shunxu][0]} order according to '{order_column}' of the row they are located, and the cell of '{select_column}' column that corresponds to the {order_type[shunxu][1]} '{order_column}' in each row is the final answer.")      
        else:
            aggregate_dict = {'sum':'the sum of {}', 'count':"the number of {}",
                            'min':'the min of {}',
                            'max':'the max of {}',
                            'avg':'the avg of {}'}
            if order_column == '*':
                multiturn.append(f"Finally, these cells are sorted in {order_type[shunxu][0]} order according to total number of each group, and the cell of '{select_column}' column with the {order_type[shunxu][1]} number is selected.")
            else:
                condition = aggregate_dict[agg].format(order_column)
                multiturn.append(f"Finally, these cells are sorted in {order_type[shunxu][0]} order according to '{condition}' of the row they are located, and the cell of '{select_column}' column that corresponds to the {order_type[shunxu][1]} '{condition}' in each row is the final answer.")         
        
    return multiturn
    


def find_position(header, contents, sql, keyword, db_path):
    def find_column(seq):
        if "*" in seq:
            return ["*"]
        else:
            output = []
            for h in header:
                if h in seq:
                    output.append(h)
            return output
    def replace_chars(s, start_index, end_index, origin, new):
        return s[:start_index] + s[start_index:end_index].replace(origin, new) + s[end_index:]
    if keyword == 'where':
        # 定义正则表达式
        # 匹配并截断字符串
        # 查找并截取字符串
        result = re.search(r"(where.*?)(?=group by)", sql, re.IGNORECASE)
        if result is None:
            result = re.search(r"(where.*?)(?=having)", sql)
        if result is None:
            result = re.search(r"(where.*?)(?=order by)", sql)
        if result is None:
            result = re.search(r"(where.*)", sql)
        
        # 提取截断后的部分
        if result:
            new_sql = result.group(1).strip()
        output = find_column(new_sql)
        return output, new_sql
    
    elif keyword == 'group by':
        pattern = r"\bgroup\s+by\b\s+(\w+)"
        match = re.search(pattern, sql)
        if match:
            item1 = match.group(1)
            return [item1], item1
    elif keyword == 'having':
        result = re.search(r"(having.*?)(?=order by)", sql, re.IGNORECASE)
        if result is None:
            result = re.search(r"(having.*)", sql)
        if result:
            new_sql = result.group(1).strip()
        output = find_column(new_sql)
        return output, new_sql
    elif keyword == 'select':
        aggregate_word = ['sum', 'count', 'min', 'max', 'avg']
        result = re.search(r"(select.*?)(?=from)", sql, re.IGNORECASE)
        if result:
            select_item = result.group(1).strip()
        column = find_column(select_item)
        
        result = re.search(r"(select.*?)(?=order by)", sql, re.IGNORECASE)
        if result is None:
            result = re.search(r"(select.*)", sql)
        if result:
            sql = result.group(1).strip()
        if any(" "+agg+" " in select_item for agg in aggregate_word):
            agg = None
            non_agg_sql = sql
            for item in aggregate_word:
                if " "+item+" " in non_agg_sql:
                    non_agg_sql = replace_chars(non_agg_sql, 0, 30, f'{item} (', '')
                    agg = item
            non_agg_sql = replace_chars(non_agg_sql, 0, 30, f')', '')
                    
            try:
                sql_output = execute_sql(db_path, non_agg_sql)
            except:
                import pdb; pdb.set_trace()
            return (column, len(sql_output)), (column, agg)
        else:
            sql_output = execute_sql(db_path, sql)
            return (column, len(sql_output)), (column, None)
    elif keyword == 'from':
        return "my_table", "my_table"
    elif keyword == 'order by':
        # 查找并截取字符串
        result = re.search(r"(order\sby.*)", sql)
        # 提取截断后的部分
        if result:
            new_sql = result.group(1).strip()
        
        output = find_column(new_sql)
        
        aggregate_word = ['sum', 'count', 'min', 'max', 'avg']
        if any(agg in new_sql for agg in aggregate_word):
            for item in aggregate_word:
                if item in new_sql:
                    agg = item
        else:
            agg = None    
        order = 'asc'
        if 'desc' in new_sql:
            order = 'desc'
        # if len(output) > 1:
        #     import pdb; pdb.set_trace()

        if output is None:
            if '*' in new_sql:
                return '*', (('*', agg), order)
        else:
            return output, ((output, agg), order)
    elif keyword == 'limit':
        pattern = r"\blimit\b\s+(\w+)"
        match = re.search(pattern, sql)
        if match:
            item1 = match.group(1)
            return item1, item1


def cover_column(contents, process):
    filter_cols, select_cols, order_cols = [], [], []
    process_keys = list(process.keys())
    for item in ['where', 'group by', 'having']:
        if item in process_keys:
            filter_cols += process[item]
    select_cols = process['select'][0]
    if 'order by' in process:
        order_cols += process['order by']
    
    select_cols_set = set(select_cols + filter_cols+order_cols)
    select_cols_num = len(select_cols_set)

    return select_cols_num

def cover_row(contents, process):

    select_rows_num = process['select'][1]    
    row_num = len(contents)
    return select_rows_num

def calculate_depth(contents, process, sql):
    depth = 0
    for key in list(process.keys()):
        if key == 'group by':
            depth += 1
        elif key == 'where':
            depth += len(process[key])
        elif key == 'having':
            depth += 1
        elif key == 'order by':
            depth += 1
    aggregate_word = ['sum', 'count', 'min', 'max', 'avg']
    for word in sql.split():
        if word.lower() in aggregate_word:
            depth += 1
    return depth

def calculate_times(process, sql):
    times = 0
    aggregate_word = ['sum', 'avg']
    math_op = ['+', '-', '*', '/']
    for word in sql.split():
        if word.lower() in aggregate_word:
            times += 1
        elif word.lower() in math_op:
            times += 1
    return times
    
        
def cover_ratio(contents, process):
    filter_cols, select_cols, order_cols = [], [], []
    process_keys = list(process.keys())
    for item in ['where', 'group by', 'having']:
        if item in process_keys:
            filter_cols += process[item]
    select_cols = process['select'][0]
    select_rows_num = process['select'][1]
    if 'order by' in process:
        order_cols += process['order by']
    
    row_num = len(contents)
    col_num = len(contents[0])
    total_cells_num = row_num * col_num
    cells_num = 0
    # where group having
    cells_num += len(filter_cols) * row_num
    # select
    if select_cols[0] not in filter_cols:
        cells_num +=  select_rows_num * 1
    
    if order_cols != [] and order_cols[0] not in (filter_cols+select_cols):
        cells_num += select_rows_num * 1
    ratio = cells_num / total_cells_num
    return ratio
    
def generate_process(header, contents, sql, path):
    order_keyword = ['join', 'from', 'where', 'group by', 'having', 'select', 'order by', 'limit']
    process, details = {}, {}
    for i, keyword in enumerate(order_keyword):
        if keyword not in sql:
            continue
        else:
            try:
                output, detail = find_position(header, contents, sql, keyword, path)
            except:
                output = []
                detail = []
            process[keyword] = output
            details[keyword] = detail
    return process, details