import argparse
import os
import random
import json
import shutil
from table_utils import generate_table, insert_random_values
from value_utils import random_int, read_json, read_jsonl, read_txt, random_big_int
from general import general_queries
from custom_template import template_queries


def main(args):
    # 判断db文件夹是否存在
    if args.new_db:
        if os.path.exists(args.db_path):
            shutil.rmtree(args.db_path)
            os.makedirs(args.db_path)
        else:
            os.makedirs(args.db_path)
    else:
        if not os.path.exists(args.db_path):
            raise Exception(f"not have this database {args.db_path}")
    
    sql_templates = []
    # 产生sql_template, 读取template文件
    if args.synthetic_mode == 'template':
        sql_templates = read_txt(args.template)
    elif args.synthetic_mode == 'general':
        general_dict = read_json(args.template)
        
    if os.path.exists(args.sql_config):
        sql_config = read_json(args.sql_config)
    else:
        sql_config = None
    # 在新database上生成数据
    if args.new_db:
        # 计算每个table需要生成多少数据
        table_number = args.total_number // args.each_table_number

        # 读取database config文件 col_min max, 
        database_config = read_json(args.database_config)
        
        # 表格的行列范围 
        column_numbers = list(range(database_config['col_min'], database_config['col_max']+1))
        row_numbers = list(range(database_config['row_min'], database_config['row_max']+1))
        
        
        ##############################################
        table_name = 'table_try' 
        table_path = os.path.join(args.db_path, table_name + '.db')
        
        # 表格随机大小
        column_number = random.choice(column_numbers)
        row_number = random.choice(row_numbers)
        
        # 生成表格schema
        while True:
            output = generate_table(args, table_path,column_number,row_number)
            if output != 0:
                break   
        
        # 表格中插入随机值 
        insert_random_values(args, table_path, column_number, row_number)
        
        multiple = 5
        while True:
            generate_sample_number = []
            for _ in range(2):
                if args.synthetic_mode == 'template':
                    data_i = template_queries(sql_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
                elif args.synthetic_mode == 'general':
                    data_i = general_queries(general_dict, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
                generate_sample_number.append(len(data_i))
            max_generate_samples = max(generate_sample_number)
            if max_generate_samples < args.each_table_number - 1:
                multiple = multiple * 5
                print(f"multiple: {multiple}")
            elif max_generate_samples >= args.each_table_number - 1:
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
            table_path = os.path.join(args.db_path, table_name + '.db')
            
            # 表格随机大小
            column_number = random.choice(column_numbers)
            row_number = random.choice(row_numbers)
            
            # 生成表格schema
            while True:
                output = generate_table(args, table_path,column_number,row_number)
                if output != 0:
                    break   
            
            # 表格中插入随机值 
            insert_random_values(args, table_path, column_number, row_number)
            
            # 根据该template生成SQL语句
            if args.synthetic_mode == 'template':
                data_i = template_queries(sql_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
            elif args.synthetic_mode == 'general':
                data_i = general_queries(general_dict, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
            data.extend(data_i)
    # 在旧database上生成数据
    else:
        # 获取该Database中所有表格
        table_names = []  # 存储文件名的列表
        for file_name in os.listdir(args.db_path):
            if os.path.isfile(os.path.join(args.db_path, file_name)):
                table_names.append(os.path.join(args.db_path, file_name))
                
                
        ##############################################
        table_path = random.choice(table_names)
        multiple = 5
        while True:
            generate_sample_number = []
            for _ in range(2):
                if args.synthetic_mode == 'template':
                    data_i = template_queries(sql_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
                elif args.synthetic_mode == 'general':
                    data_i = general_queries(general_dict, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
                generate_sample_number.append(len(data_i))
            max_generate_samples = max(generate_sample_number)
            if max_generate_samples < args.each_table_number - 1:
                multiple = multiple * 5
                print(f"multiple: {multiple}")
            elif max_generate_samples >= args.each_table_number - 1:
                break
        ############################################ 
                
                
        
        # 根据该表格生成SQL语句
        data = []
        for i in range(args.total_number // args.each_table_number):
            print(str(i) + '\n')
            table_path = random.choice(table_names)
            
            if args.synthetic_mode == 'template':
                data_i = template_queries(sql_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
            elif args.synthetic_mode == 'general':
                data_i = general_queries(general_dict, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
            
            data.extend(data_i)
            
    # 保存生成的数据
    random.shuffle(data)
    print(len(data))
    if args.des == '':
        save_path = f"./data/{os.path.basename(args.db_path)}_{len(data)}_{random_big_int()}_{os.path.basename(args.template).replace('.txt','').replace('.json','')}.json"
    else:
        save_path = f"./data/{os.path.basename(args.db_path)}_{len(data)}_{args.des}_{os.path.basename(args.template).replace('.txt','').replace('.json','')}.json"
    with open(save_path, 'w') as f:
        for item in data:
            json.dump(item, f)
            f.write('\n')
    print(f"saving to {save_path}")
    
  

    



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--db_path', type=str, default='./db/db1')
    parser.add_argument('--new_db', type=int, default=1)
    parser.add_argument('--total_number', type=int, default=1000)
    parser.add_argument('--each_table_number', type=int, default=50)
    parser.add_argument('--database_config', type=str, default='./config/database_config.json')
    parser.add_argument('--sql_config', type=str, default='./config/sql_config.json')
    parser.add_argument('--synthetic_mode', choices=['template', 'general'], default='general')
    parser.add_argument('--template', type=str, default='./template/general.json')
    parser.add_argument('--data_mode', choices=['ft', 'eval'], default='eval')
    parser.add_argument('--des', type=str, default='')
    args = parser.parse_args()

    main(args)