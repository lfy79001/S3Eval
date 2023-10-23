import re
from .table_utils import execute_sql



def filter_times(sql):
    times = 0
    filter_words = ['=', '>', '<', 'in', 'like']
    for word in sql.split():
        if word.lower() in filter_words:
            times += 1
    return times

def calculate_times(sql):
    times = 0
    aggregate_word = ['sum', 'avg']
    math_op = ['+', '-', '*', '/']
    for word in sql.split():
        if word.lower() in ['sum', 'count', 'min', 'max']:
            times += 1
        elif word.lower() == 'avg':
            times += 2
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
