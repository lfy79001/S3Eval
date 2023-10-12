import json, os
import openai
import argparse
import pandas as pd
import tiktoken
import time
import sys
from transformers import AutoTokenizer, AutoModel, LlamaTokenizer, LlamaForCausalLM, BitsAndBytesConfig
import torch
from accelerate import Accelerator
def find_element_position(contents, new_answer):
    for row_index, row in enumerate(contents):
        for col_index, element in enumerate(row):
            if str(element) == str(new_answer):
                return row_index, col_index
    return -1, -1 

def num_tokens_from_string(table, tokenizer):
    return len(tokenizer.tokenize(table))

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
    
def generate_table(data):
    header = data['header']
    contents = data['contents']
    header_string = "col : " + " | ".join(data["header"]) + " "
    value_string = ""
    for i, row in enumerate(data['contents']):
        value_string += "row " + str(i+1) + " : "
        row_cell_values = [str(cell_value) if isinstance(cell_value, int) else cell_value.lower()
                           for cell_value in row]
        value_string += " | ".join(row_cell_values) + " "
    output_string = header_string + value_string
    return output_string

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

def generate_flatten_norow(data):
    header = data['header']
    contents = data['contents']
    header_string = f'The table have {len(header)} columns: '
    header_string += " | ".join(header) + '\n'
    value_string = ""
    for i, row in enumerate(contents):
        value_string += ""
        row_cell_values = [str(cell_value) if isinstance(cell_value, int) else cell_value.lower()
                           for cell_value in row]
        row_value_string = ""
        for j, value in enumerate(row_cell_values):
            row_value_string += f"{header[j]} is {value}. "
        value_string += row_value_string + '\n'
    output_string = header_string + value_string
    return output_string

def read_jsonl(path):
    total_data = []
    with open(path, 'r') as f:
        for line in f:
            data = json.loads(line)
            total_data.append(data)
    return total_data


def main(args):
    tokenizer = AutoTokenizer.from_pretrained(args.model_path, trust_remote_code=True)
    if ('70b' in args.model_path) or args.bit8:
        config_kwargs = {}
        config_kwargs['load_in_8bit'] = True
        config_kwargs['quantization_config'] = BitsAndBytesConfig(
            load_in_8bit=True,
            llm_int8_threshold=6.0
        )
        model = LlamaForCausalLM.from_pretrained(args.model_path, device_map="auto", **config_kwargs)
    else:
        model = LlamaForCausalLM.from_pretrained(args.model_path, torch_dtype=torch.bfloat16, device_map="auto")
        
    accelerator = Accelerator()
    device = accelerator.device

    model = model.eval()

    input_format = generate_prompt_format(args)
    
    json_data = read_jsonl(args.file_name)
    
    if args.test_number:
        json_data = json_data[:args.test_number]

    outputs = []
    for i, data in enumerate(json_data):
        if args.format == 'markdown':
            df = pd.DataFrame(data['contents'], columns=data['header'])
            table_str = df.to_markdown(headers=data['header'], tablefmt="pipe")
        elif args.format == 'flatten':
            table_str = generate_flatten(data)
        elif args.format == 'flatten_norow':
            table_str = generate_flatten_norow(data)
        table = table_str

        cnt = 0
        while num_tokens_from_string(table, tokenizer) > args.max_length:
            table = " ".join(table.split()[:args.max_length - cnt]) # chunk the input len into 16k tokens
            cnt += 50
        
        table_len = num_tokens_from_string(table, tokenizer)
         
        messages = []
        
        if args.task == 'qa':
            examples = data['examples']
            fewshot_input = ""
            for example in examples[1:args.n_shot+1]:
                fewshot_input += "Question:{}\nAnswer:{}\n".format(example['question'], example['answer'])
            fewshot_input += f"Question:{examples[0]['question']}\nAnswer:"
            gold_answer = examples[0]['answer']
            input_prompt = input_format.format(table, fewshot_input)
        elif args.task == 'sql':
            examples = data['examples']
            fewshot_input = ""
            for example in examples[1:args.n_shot+1]:
                fewshot_input += "SQL:{}\nAnswer:{}\n".format(example['sql'], example['answer'])
            fewshot_input += f"SQL:{examples[0]['sql']}\nAnswer:"
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
 
        inputs = tokenizer(input_prompt, return_tensors="pt").to(device)

        sample = model.generate(**inputs, do_sample=False, max_new_tokens=args.max_new_tokens)
        prompt_length = inputs.input_ids.size()[-1]

        response = tokenizer.decode(sample[0][prompt_length:])
        response = response.replace('</s>', '')
        
        print(i, '[output]:', response, '[ground truth]:', gold_answer)
        if args.task == 'qa':
            keywords = ['\n\n', '\nPlease','Please execute', 'Please provide', 'Note:', 'Question:','\nQuestion', 'Instruction:', '\nInstruction:']
            end_index = -1
            for keyword in keywords:
                end_index = response.find(keyword)   
                if end_index != -1:
                    break 
        elif args.task == 'sql':
            keywords = ['\n\n', '\nPlease','Please execute', 'Please provide', 'Note:', 'SQL:','\nSQL']
            end_index = -1
            for keyword in keywords:
                end_index = response.find(keyword)   
                if end_index != -1:
                    break 
        
        if end_index != -1:
            result = response[:end_index].strip('\n')
        else:
            result = response.strip('\n')
        row_index, _ = find_element_position(data['contents'], gold_answer)

        outputs.append({'pred':result, 'gold': gold_answer, 'answer_row':row_index, 'sql':examples[0]['sql'], 'contents': data['contents']}) 
    
    save_path = f"./results/llama2_{args.task}_{args.format}_{args.test_number}_{args.n_shot}_{args.file_name.split('/')[-1].replace('.json','')}.json"
    with open(save_path, 'w') as f:
        json.dump(outputs, f, indent=2)
        
    print(f'saving to {save_path}')


    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--task', choices=["qa", "sql", "cot"], default='sql')
    parser.add_argument('--format', choices=["markdown", "flatten", "flatten_norow"], default='flatten')
    parser.add_argument('--file_name', type=str, default='../data/wtq.json')
    parser.add_argument('--max_length', type=int, default=4000)
    parser.add_argument('--max_new_tokens', type=int, default=30)
    parser.add_argument('--model_path', type=str, default="meta-llama/Llama-2-7b-hf")
    parser.add_argument('--test_number', type=int, default=1000)
    parser.add_argument('--n_shot', type=int, default=5)
    parser.add_argument('--bit8', type=bool, default=False)
    args = parser.parse_args()
    print(args.task)
    print(args)
    main(args)