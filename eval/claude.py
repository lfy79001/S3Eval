import json, os
import openai
import argparse
import pandas as pd
import tiktoken
import time
import sys
from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT


import os
def find_element_position(contents, new_answer):
    for row_index, row in enumerate(contents):
        for col_index, element in enumerate(row):
            if str(element) == str(new_answer):
                return row_index, col_index
    return -1, -1  # not found

def is_numeric(string):
    try:
        float(string)
        return True
    except ValueError:
        return False
def convert_to_number(string):
    if '.' in string:
        return float(string)
    else:
        return int(string)

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    encoding = tiktoken.encoding_for_model(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def generate_prompt_format(args):
    if args.task == 'qa':
        if args.n_shot != 0:
            return 'You need to obtain the final answer based on the table and questions. Only give me the answer and do not output any other words. \nTable:\n{}\nNow you need to get the answer based on the question, only give me the answer and do not output any other words.\nThe following are some examples.\n\n{}'
        else:
            return 'You need to obtain the final answer based on the table and questions. Only give me the answer and do not output any other words. \nTable:\n{}\nNow you need to get the answer based on the question, only give me the answer and do not output any other words.\n{}'
    elif args.task == 'sql':
        if args.n_shot != 0:
            return "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable:\n{}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\nThe following are some examples.\n\n{}"
        else:
            return "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable:\n{}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\n{}"
    elif args.task == 'cot':
        return "You are an SQL executor, you need to output the execution process and the final answer based on the table and SQL. \nTable:\n{}\nNow you need to get the answer based on the SQL, only give me the intermedium results and the final answer.\nThe following are some examples.\n\n{}"
    

def read_jsonl(path):
    total_data = []
    with open(path, 'r') as f:
        for line in f:
            data = json.loads(line)
            total_data.append(data)
    return total_data
def read_json(path):
    with open(path, 'r') as f:
        total_data = json.load(f)
    return total_data

def generate_flatten(data):
    header = data['header']
    contents = data['contents']
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


def generate_noenter(data):
    header = data['header']
    contents = data['contents']
    header_string = f'The table have {len(header)} columns: '
    header_string += " | ".join(header) + ' '
    value_string = ""
    for i, row in enumerate(contents):
        value_string += "row " + str(i+1) + " : "
        row_cell_values = [str(cell_value) if isinstance(cell_value, int) else cell_value.lower()
                           for cell_value in row]
        row_value_string = ""
        for j, value in enumerate(row_cell_values):
            row_value_string += f"{header[j]} is {value}. "  f" {value} | "
        value_string += row_value_string + ' '
    output_string = header_string + value_string
    return output_string


def main(args):
    input_format = generate_prompt_format(args)
    
    json_data = read_jsonl(args.file_name)
    
    if args.test_number:
        json_data = json_data[:args.test_number]
        
    anthropic = Anthropic(
        api_key="<your-api-key>",
    )
        
        
    outputs = []
    for i, data in enumerate(json_data):
        if args.format == 'markdown':
            df = pd.DataFrame(data['contents'], columns=data['header'])
            table_str = df.to_markdown(headers=data['header'], tablefmt="pipe")
        elif args.format == 'no_enter':
            df = pd.DataFrame(data['contents'], columns=data['header'])
            table_str = df.to_markdown(headers=data['header'], tablefmt="pipe")
            table_str = table_str.replace('\n', ' ')
        elif args.format == 'flatten':
            table_str = generate_flatten(data)
        table = table_str
        cnt = 0
        # while num_tokens_from_string(table, "gpt-3.5-turbo") > args.max_length:
        #     table = " ".join(table.split()[:args.max_length - cnt]) # chunk the input len into 16k tokens
        #     cnt += 500

        ################    #################    
        messages = []

        if args.task == 'qa':
            examples = data['examples']
            fewshot_input = ""
            for example in examples[1:args.n_shot+1]:
                fewshot_input += "Question:{}\nAnswer:{}\n".format(example['question'], example['answer'])
            fewshot_input += f"Question:{examples[0]['question']}"
            gold_answer = examples[0]['answer']
            input_prompt = input_format.format(table, fewshot_input)
        elif args.task == 'sql':
            examples = data['examples']
            fewshot_input = ""
            for example in examples[1:args.n_shot+1]:
                fewshot_input += "SQL:{}\nAnswer:{}\n".format(example['sql'], example['answer'])
            fewshot_input += f"SQL:{examples[0]['sql']}"
            gold_answer = examples[0]['answer']
            input_prompt = input_format.format(table, fewshot_input)
        elif args.task == 'cot':
            examples = data['examples']
            fewshot_input = ""
            for example in examples[1:args.n_shot+1]:
                fewshot_input += "SQL:{}\nExecution process:\n{}\n\n".format(example['sql'],example['sql_cot'])
            fewshot_input += f"SQL:{examples[0]['sql']}\nExecution process:\n"
            gold_answer = examples[0]['answer']
            input_prompt = input_format.format(table, fewshot_input)

        result = ""
        for _ in range(10):
            try:
                completion = anthropic.completions.create(
                    model="claude-1.3-100k",
                    max_tokens_to_sample=args.max_new_tokens,
                    prompt=f"{HUMAN_PROMPT} {input_prompt} {AI_PROMPT}",
                    temperature=0,
                )
                result = completion.completion
                result = result.strip()  # get the paraphrased answer
                print(i, '[output]:', result, '[ground truth]:', gold_answer)
                break
            except KeyboardInterrupt:
                print("exit")
                exit()
            except Exception as e:  # add some logit here for retry
                time.sleep(0.1)
        
        row_index, _ = find_element_position(data['contents'], gold_answer)
        if args.task in ['sql', 'qa']:
            answer = result
        elif args.task == 'cot':
            sql_length = len(examples[0]['sql'].split(' '))
            answer = result[result.find('Answer:')+len('Answer:'):] 
            
        
                       
        outputs.append({'pred':answer, 'gold': gold_answer, 'answer_row':row_index, 'sql':examples[0]['sql'], 'contents': data['contents']})  
        time.sleep(1.0)
        ##########################################
    save_path = f"./results/claude_{args.task}_{args.format}_{args.test_number}_{args.n_shot}_{args.file_name.split('/')[-1].replace('.json','')}.json"
    with open(save_path, 'w') as f:
        json.dump(outputs, f, indent=2)
    print(f"saving to {save_path}")

    

if __name__ == '__main__':
    os.environ["ANTHROPIC_API_KEY"] = "your-api-key"
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', choices=["qa", "sql", "cot"], default='sql')
    parser.add_argument('--format', choices=["markdown", "flatten", "no_enter"], default='flatten')
    parser.add_argument('--file_name', type=str, default='../data/<data_file>.json')
    parser.add_argument('--max_length', type=int, default=16000)
    parser.add_argument('--max_new_tokens', type=int, default=30)
    parser.add_argument('--test_number', type=int, default=1000)
    parser.add_argument('--n_shot', type=int, default=5)
    args = parser.parse_args()
    main(args)    
