import json, os
import openai
import argparse
import pandas as pd
import tiktoken
import time
import sys
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import torch
from accelerate import Accelerator
import transformers


def num_tokens_from_string(table, tokenizer):
    return len(tokenizer.tokenize(table))

def generate_prompt_format(args):
    if args.task == 'qa':
        if args.n_shot != 0:
            return 'You need to obtain the final answer based on the table and instructions. Only give me the result and do not output any other words. \nTable:\n{}\nNow you need to get the answer based on the instruction, only give me the result and do not output any other words.\nThe following are some examples.\n\n{}'
        else:
            return 'You need to obtain the final answer based on the table and instructions. Only give me the result and do not output any other words. \nTable:\n{}\nNow you need to get the answer based on the instruction, only give me the result and do not output any other words.\n{}'
    elif args.task == 'sql':
        if args.n_shot != 0:
            return "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable:\n{}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\nThe following are some examples.\n\n{}"
        else:
            return "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable:\n{}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\n{}"

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

def read_jsonl(path):
    total_data = []
    with open(path, 'r') as f:
        for line in f:
            data = json.loads(line)
            total_data.append(data)
    return total_data


def main(args):
    tokenizer = AutoTokenizer.from_pretrained(args.model_path, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(args.model_path, torch_dtype=torch.bfloat16, device_map="auto")

        
    accelerator = Accelerator()
    device = accelerator.device
    

    input_format = generate_prompt_format(args)
    
    json_data = read_jsonl(args.file_name)
    
    if args.mode == 'toy':
        json_data = json_data[:100]

    outputs = []
    for i, data in enumerate(json_data):
        if args.format == 'markdown':
            df = pd.DataFrame(data['contents'], columns=data['header'])
            table_str = df.to_markdown(headers=data['header'], tablefmt="pipe")
            # table_str = generate_table(data)
        elif args.format == 'flatten':
            pass
        table = table_str

        cnt = 0
        while num_tokens_from_string(table, tokenizer) > args.max_length:
            table = " ".join(table.split()[:args.max_length - cnt]) # chunk the input len into 16k tokens
            cnt += 500
        
        table_len = num_tokens_from_string(table, tokenizer)
         
        messages = []
        
        if args.task == 'qa':
            examples = data['examples']
            fewshot_input = ""
            for example in examples[1:args.n_shot+1]:
                fewshot_input += "Instruction:{}\nAnswer:{}\n".format(' '.join(example['multiturn']), example['answer'])
            fewshot_input += f"Instruction:{' '.join(examples[0]['multiturn'])}\nAnswer:"
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
            
        inputs = tokenizer(input_prompt, return_tensors="pt").to(device)
        inputs.pop('token_type_ids', None)  # 从inputs中删除token_type_ids键
        sample = model.generate(**inputs, do_sample=False, max_new_tokens=30)
        prompt_length = inputs.input_ids.size()[-1]

        response = tokenizer.decode(sample[0][prompt_length:])
        response = response.replace('</s>', '')

        print(i, '[output]:', response, '[ground truth]:', gold_answer)
        
        if args.task == 'qa':
            keywords = ['\n\n', '\nPlease','Please execute', 'Please provide', 'Note:', 'SQL:','\nSQL', 'Instruction:', '\nInstruction:']
            end_index = -1
            for keyword in keywords:
                end_index = response.find(keyword)   
                if end_index != -1:
                    break 
        elif args.task == 'sql':
            keywords = ['\n\n', '\nPlease','Please execute', 'Please provide', 'Note:', 'SQL:','\nSQL','The execution results']
            end_index = -1
            for keyword in keywords:
                end_index = response.find(keyword)   
                if end_index != -1:
                    break 
        
        if end_index != -1:
            result = response[:end_index].strip('\n')
        else:
            result = response.strip('\n')


        outputs.append({'pred':result, 'gold': gold_answer}) 
        
    save_path = f"./results/falcon_{args.task}_{args.n_shot}.json"
    with open(save_path, 'w') as f:
        json.dump(outputs, f, indent=2)
        
    print(f'saving to {save_path}')


    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--task', choices=["qa", "sql"], default='qa')
    parser.add_argument('--format', choices=["markdown", "flatten"], default='markdown')
    parser.add_argument('--file_name', type=str, default='../data/db_long_small_2803_eval.json')
    parser.add_argument('--max_length', type=int, default=6000)
    parser.add_argument('--max_new_tokens', type=int, default=30)
    parser.add_argument('--mode', choices=["toy", "baby", "full"],default="toy")
    parser.add_argument('--model_path', type=str, default="tiiuae/falcon-40b-instruct")
    parser.add_argument('--n_shot', type=int, default=3)
    parser.add_argument('--bit8', type=bool, default=False)
    args = parser.parse_args()
    print(args.task)
    main(args)
    # python llama2_fewshot.py --task qa --format markdown --n_shot 4 --mode toy