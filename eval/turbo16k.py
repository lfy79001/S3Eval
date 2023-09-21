import json, os
import openai
import argparse
import pandas as pd
import tiktoken
import time
import sys

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
            return 'You need to obtain the final answer based on the table and instructions. Only give me the result and do not output any other words. \nTable:\n{}\nNow you need to get the answer based on the instruction, only give me the result and do not output any other words.\nThe following are some examples.\n\n{}'
        else:
            return 'You need to obtain the final answer based on the table and instructions. Only give me the result and do not output any other words. \nTable:\n{}\nNow you need to get the answer based on the instruction, only give me the result and do not output any other words.\n{}'
    elif args.task == 'sql':
        if args.n_shot != 0:
            return "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable:\n{}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\nThe following are some examples.\n\n{}"
        else:
            return "You are an SQL executor, you need to execute SQL based on the give table and SQL statement to obtain the execution results. Only give me the execution results and do not output any other words. \nTable:\n{}\nNow you need to execute SQL based on the given table and SQL statement to obtain the execution result. Only give me the result and do not output any other words or SQL statement.\n{}"

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

def main(args):
    input_format = generate_prompt_format(args)
    
    json_data = read_jsonl(args.file_name)
    
    if args.mode == 'toy':
        json_data = json_data[:100]
        
    outputs = []
    for i, data in enumerate(json_data):
        if args.format == 'markdown':
            df = pd.DataFrame(data['contents'], columns=data['header'])
            table_str = df.to_markdown(headers=data['header'], tablefmt="pipe")
        elif args.format == 'flatten':
            pass
        table = table_str
        cnt = 0
        while num_tokens_from_string(table, "gpt-3.5-turbo") > args.max_length:
            table = " ".join(table.split()[:args.max_length - cnt]) # chunk the input len into 16k tokens
            cnt += 500

        ################    #################    
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
        import pdb; pdb.set_trace()
        messages.append({"role": "user", "content": input_prompt})


        for _ in range(10):
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo-16k-0613",
                    messages=messages, 
                    max_tokens=args.max_new_tokens,
                    temperature=0.0001,
                )  # get response
                result = response['choices'][0]['message']['content']
                result = result.strip()  # get the paraphrased answer

                print(i, '[output]:', result, '[ground truth]:', gold_answer)
                
                break
            except Exception as e:  # add some logit here for retry
                if isinstance(e, KeyboardInterrupt):
                    raise e
                time.sleep(0.1)
        outputs.append({'pred':result, 'gold': gold_answer})  
        time.sleep(1.0)
        ##########################################
    save_path = f"./results/turbo16k_{args.task}_{args.n_shot}_sqleval_100.json"
    with open(save_path, 'w') as f:
        json.dump(outputs, f, indent=2)
    print(f"saving to {save_path}")

    

if __name__ == '__main__':
    openai.api_key = "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"
    parser = argparse.ArgumentParser()

    parser.add_argument('--task', choices=["qa", "sql"], default='qa')
    parser.add_argument('--format', choices=["markdown", "flatten"], default='markdown')
    parser.add_argument('--file_name', type=str, default='../data/db_long_small_2803_eval.json')
    parser.add_argument('--max_length', type=int, default=15000)
    parser.add_argument('--max_new_tokens', type=int, default=100)
    parser.add_argument('--mode', choices=["toy", "baby", "full"], default="toy")
    parser.add_argument('--n_shot', type=int, default=0)
    args = parser.parse_args()
    main(args)
    # python turbo16k_reval.py 400_6_long_1568
    
    
    
