import json
import pandas as pd
import tiktoken
from tqdm import tqdm

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    encoding = tiktoken.encoding_for_model(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


# num_tokens_from_string(table_passage, "gpt-3.5-turbo")

with open('TableQAEval.json', 'r') as file:
    data = json.load(file)
total = []
s1 = []
s2 = []
s3 = []
for d in tqdm(data):
    df = pd.DataFrame(d['contents'], columns=d['header'])
    table = df.to_markdown(headers=d['header'], tablefmt="pipe")
    if d['source'] != 'structured':
        table_passage = table + d['passage']
    else:
        table_passage = table
        
    number = num_tokens_from_string(table_passage, "gpt-3.5-turbo")
    if d['source'] == 'numerical':
        s1.append(number)
    elif d['source'] == 'multihop':
        s2.append(number)
    elif d['source'] == 'structured':
        s3.append(number)
    total.append(number)

print(f"numerical {len(s1)}  {sum(s1)/len(s1)}")
print(f"multihop {len(s2)}  {sum(s2)/len(s2)}")
print(f"structured {len(s3)}  {sum(s3)/len(s3)}")
print(f"total {len(total)}  {sum(total)/len(total)}")