import string
import random
from datetime import datetime, timedelta
import json
import re

def has_duplicates(lst):
    # 使用集合（set）来判断列表中的元素是否唯一
    return len(lst) != len(set(lst))

def random_string():
    letters = string.ascii_lowercase
    length = random.randint(4,9)
    return ''.join(random.choice(letters) for _ in range(length))

def random_strings(number):
    random_strings = []
    for i in range(number):
        random_strings.append(random_string())
    return random_strings

def random_int():
    return random.randint(2, 327)
def random_big_int():
    return random.randint(2, 1000)

def random_float():
    random_float = random.uniform(1.1, 20.0)
    formatted_float = "{:.2f}".format(random_float)  # 设置为2位小数
    return formatted_float


def generate_random_date():
    start_date = "2001-02-13"
    end_date = "2023-08-31"
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    time_between_dates = end_date - start_date
    random_number_of_days = random.randrange(time_between_dates.days)
    random_date = start_date + timedelta(days=random_number_of_days)

    return random_date.strftime("%Y-%m-%d")

# 生成随机的出生年月日
def generate_random_date_of_birth(age):
    # 获取当前日期
    current_date = datetime.now()

    # 计算出生日期的起始和结束日期范围
    end_date = current_date - timedelta(days=365 * age)
    start_date = end_date - timedelta(days=365)

    # 生成随机的出生日期
    random_date = random_date_between(start_date, end_date)

    return random_date.strftime('%Y-%m-%d')

# 在日期范围内生成随机日期
def random_date_between(start_date, end_date):
    start_timestamp = start_date.timestamp()
    end_timestamp = end_date.timestamp()

    random_timestamp = random.uniform(start_timestamp, end_timestamp)

    return datetime.fromtimestamp(random_timestamp)


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

def read_txt(path):
    output = []
    with open(path, 'r') as file:
        for line in file:
            line = line.strip()
            line_list = line.split()
            output.append(line_list)
    return output

def save_txt(path, data):
    with open(path, 'w') as file:
        for line in data:
            file.write(line + '\n')


def random_with_weight(elements, weights):
    return random.choices(elements, weights=weights, k=1)[0]

def random_double(strings, weights):
    result = [string for string, weight in zip(strings, weights) for _ in range(weight)]
    random.shuffle(result)
    return result

def random_dict_key(my_dict):
    random_key = random.choice(list(my_dict.keys()))
    return random_key

def random_dict_value(my_dict):
    random_value = random.choice(list(my_dict.values()))
    return random_value

def random_dict_key_value(my_dict):
    random_key = random.choice(list(my_dict.keys()))
    return random_key, my_dict[random_key]


# 删除重复空格
def remove_double_spaces(text):
    pattern = r' {2}'  # 匹配连续的两个空格
    replaced_text = re.sub(pattern, ' ', text)
    return replaced_text

# 解析字符串中的[s1,s2]，用于处理嵌套数据
def extract_subsql_position(text):
    pattern = r'\[(.*?)\]'  # 匹配方括号内的内容
    match = re.search(pattern, text)
    if match:
        content = match.group(0)  # 获取匹配到的内容，包括方括号 "[ ]"
        items = [item.strip() for item in match.group(1).split(',')]  # 拆分并去除空格
        return content, items
    else:
        return None, None
    
    
def merge_dicts(dict1, dict2):
    merged_dict = {}
    for key in dict1.keys() & dict2.keys():
        merged_dict[key] = list(set(dict1[key] + dict2[key]))
    return merged_dict

