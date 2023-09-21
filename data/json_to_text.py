import json

def read_jsonl(path):
    total_data = []
    with open(path, 'r') as f:
        for line in f:
            data = json.loads(line)
            total_data.append(data)
    return total_data

def save_txt(path, data):
    with open(path, 'w') as file:
        for line in data:
            file.write(line + '\n')

path = 'db1_500_23_general.json'
json_data = read_jsonl(path)


sqls = [ data['examples'][0]['sql'] for data in json_data]

save_txt(path.replace('.json','.txt'), sqls)


