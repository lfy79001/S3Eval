from s3eval.value_utils import read_txt, read_jsonl, save_txt


import sys
file_path = sys.argv[1]

json_data = read_jsonl(file_path)


outputs = []
answer_outputs = []
for data in json_data:
    outputs.append(data['examples'][0]['sql'])
    answer_outputs.append(str(data['examples'][0]['answer']))

save_txt(file_path.replace('.json', '.txt'), outputs)
save_txt(file_path.replace('.json', '_answer.txt'), answer_outputs)