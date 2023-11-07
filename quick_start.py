import argparse
import os
import random
import json
import shutil
from s3eval.table_utils import generate_table, insert_random_values, generate_database_config, delete_table, get_table_length
from s3eval.value_utils import random_int, read_json, read_jsonl, read_txt, random_big_int
from s3eval.general import general_queries
from s3eval.custom_template import template_queries


class S3Eval:
    def __init__(self, data_type, process=False, multistep=False, cot=False):
        db_path='./db/db1'

        each_table_number=10 
        
        self.db_path = db_path

        self.each_table_number = each_table_number
        self.data_type = data_type
        

        self.database_config = {}        
        self.sql_config = {}
        self.template = "./template/general.json"
        
        if data_type == "general":
            self.template = "./template/general.json"
            self.database_config = read_json("./config/fixed/general_database_config.json")
            self.sql_config = read_json("./config/fixed/general_sql_config.json")
        elif data_type == "easy":
            self.template = "./template/easy.txt"
            self.database_config = read_json("./config/fixed/easy_database_config.json")
            self.sql_config = read_json("./config/fixed/easy_sql_config.json")
        elif data_type.startswith("long"):
            self.template = "./template/long_context.txt"
            self.database_config = read_json("./config/fixed/long_database_config.json")
            self.sql_config = read_json("./config/fixed/long_sql_config.json")
            if data_type == "long2k":
                self.database_config["row_min"] = 30
                self.database_config["row_max"] = 30
            elif data_type == "long4k":
                self.database_config["row_min"] = 65
                self.database_config["row_max"] = 65
            elif data_type == "long8k":
                self.database_config["row_min"] = 140
                self.database_config["row_max"] = 140
            elif data_type == "long16k":
                self.database_config["row_min"] = 290
                self.database_config["row_max"] = 290
            elif data_type == "long32k":
                self.database_config["row_min"] = 600
                self.database_config["row_max"] = 600
            elif data_type == "long64k":
                self.database_config["row_min"] = 1200
                self.database_config["row_max"] = 1200
            elif data_type == "long128k":
                self.database_config["row_min"] = 2400
                self.database_config["row_max"] = 2400
        elif data_type == "custom":
            self.template = ""
            self.database_config = ""
            self.sql_config = "" 
            self.context_length = 0
            self.context_length_format = ""
            self.tokenizer = ""

        if process: self.sql_config["output_config"]["process"] = True
        if multistep: self.sql_config["output_config"]["multistep"] = True
        if cot: self.sql_config["output_config"]["cot"] = True
        
    # def set_template(self, path):
    #     self.path = path
    # def set_database_config(self, database_config):
    #     self.database_config = database_config
    # def set_sql_config(self, sql_config):
    #     self.sql_config = sql_config
    # def set_context_length(self, context_length):
    #     self.context_length = context_length
    # def set_context_length_format(self, context_length_format):
    #     self.context_length_format = context_length_format
    # def set_tokenizer(self, tokenizer):
    #     self.tokenizer = tokenizer
        
    def generate_data(self, total_number, output_path):
        # 判断db文件夹是否存在
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
            os.makedirs(self.db_path)
        else:
            os.makedirs(self.db_path)

        
        sql_templates = None
        # 产生sql_template, 读取template文件
        if self.template.endswith('.txt'):
            sql_templates = read_txt(self.template)
        elif self.template.endswith('.json'):
            sql_templates = read_json(self.template)
            

        # 在新database上生成数据

        # 计算每个table需要生成多少数据
        table_number = total_number // self.each_table_number
        
    
        # 表格的行列范围 
        column_numbers = list(range(self.database_config['col_min'], self.database_config['col_max']+1))
        row_numbers = list(range(self.database_config['row_min'], self.database_config['row_max']+1))
        
        ##############################################
        table_name = 'table_try' 
        table_path = os.path.join(self.db_path, table_name + '.db')
        
        # 表格随机大小
        column_number = random.choice(column_numbers)
        row_number = random.choice(row_numbers)
        
        # 生成表格schema
        while True:
            output = generate_table(self.database_config, table_path, column_number,row_number)
            if output != 0:
                break   
        
        # 表格中插入随机值 
        insert_random_values(self.database_config, table_path, column_number, row_number)
        
        multiple = 5
        while True:
            generate_sample_number = []
            for _ in range(2):
                if self.template.endswith('.txt'):
                    data_i = template_queries(sql_templates, self.each_table_number, table_path, self.sql_config, multiple=multiple, data_mode="eval")
                elif self.template.endswith('.json'):
                    data_i = general_queries(sql_templates, self.each_table_number, table_path, self.sql_config, multiple=multiple, data_mode="eval")
                generate_sample_number.append(len(data_i))
            max_generate_samples = max(generate_sample_number)
            if max_generate_samples < self.each_table_number - 1:
                multiple = multiple * 5
                print(f"multiple: {multiple}")
            elif max_generate_samples >= self.each_table_number - 1:
                break

        try:
            os.remove(table_path)
            print(f"The file {table_path} has been successfully deleted.")
        except FileNotFoundError:
            print(f"The file {table_path} does not exist.")
        except PermissionError:
            print(f"Permission denied to delete the file {table_path}.")
        except Exception as e:
            print(f"An error occurred while deleting the file {table_path}: {str(e)}")        
    
        ############################################ 
        
        
        
        data = []
        for i in range(table_number):
            print(str(i) + '\n')
            table_name = 'table' + str(i)
            table_path = os.path.join(self.db_path, table_name + '.db')
            
            # 表格随机大小
            column_number = random.choice(column_numbers)
            row_number = random.choice(row_numbers)
            
            # 生成表格schema
            while True:
                output = generate_table(self.database_config, table_path,column_number,row_number)
                if output != 0:
                    break   
            
            # 表格中插入随机值 
            insert_random_values(self.database_config, table_path, column_number, row_number)
            
            # 根据该template生成SQL语句
            if self.template.endswith('.txt'):
                data_i = template_queries(sql_templates, self.each_table_number, table_path, self.sql_config, multiple=multiple, data_mode="eval")
            elif self.template.endswith('.json'):
                data_i = general_queries(sql_templates, self.each_table_number, table_path, self.sql_config, multiple=multiple, data_mode="eval")
            data.extend(data_i)
            
        # 保存生成的数据
        random.shuffle(data)
        print(len(data))

        save_path = output_path
        with open(save_path, 'w') as f:
            for item in data:
                json.dump(item, f)
                f.write('\n')
        print(f"saving to {save_path}")
        return data
    
    
    
s3eval = S3Eval("general")  # general, easy, long2k, long4k, long8k, long16k
output_path = "./data/general1.json"
data = s3eval.generate_data(500, output_path)