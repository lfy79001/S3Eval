import json
from tqdm import tqdm
import tiktoken
import pandas as pd
import sqlite3
import os
import random
import string
import argparse
import re
import pandas as pd
import sys
from transformers import AutoTokenizer



def read_jsonl(path):
    total_data = []
    with open(path, 'r') as f:
        for line in f:
            data = json.loads(line)
            total_data.append(data)
    return total_data

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    encoding = tiktoken.encoding_for_model(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def generate_flatten(data):
    header = data['header']
    contents = data['contents']
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
    output_string = header_string + value_string

    return output_string



tongji = []
path = sys.argv[1]
mode = sys.argv[2]
model = sys.argv[3]
tokenizer = ''
if model == 'llama2':
    model_path = "NousResearch/Yarn-Llama-2-7b-64k"
    tokenizer = AutoTokenizer.from_pretrained(model_path)

json_data = read_jsonl(path)
for data in json_data:
    if mode == 'markdown':
        df = pd.DataFrame(data['contents'], columns=data['header'])
        table_str = df.to_markdown(headers=data['header'], tablefmt="pipe")
    elif mode == 'flatten':
    # import pdb; pdb.set_trace()
        table_str = generate_flatten(data)
    if model == 'gpt':
        number = num_tokens_from_string(table_str, "gpt-3.5-turbo")
    elif model in ['llama', 'llama2']:
        number = len(tokenizer.tokenize(table_str))
    tongji.append(number)
    
    
print(len(tongji))
print(sum(tongji) / len(tongji))
