import argparse
import os
import random
import json
import shutil
from s3eval.table_utils import generate_table, insert_random_values, generate_database_config, delete_table, get_table_length
from s3eval.value_utils import random_int, read_json, read_jsonl, read_txt, random_big_int
from s3eval.fine_template import template_queries
from s3eval.value_utils import random_with_weight

def main(args):
    if args.language == "en":
        from s3eval.coarse_template import general_queries
    elif args.language == "zh":
        from s3eval.coarse_template_zh import general_queries
    
    
    # Check if the "db" folder exists
    if args.new_db:
        if os.path.exists(args.db_path):
            shutil.rmtree(args.db_path)
            os.makedirs(args.db_path)
        else:
            os.makedirs(args.db_path)
    else:
        if not os.path.exists(args.db_path):
            raise Exception(f"not have this database {args.db_path}")
    
    templates = read_txt(args.template)
    fine_templates, coarse_templates = [], []
    for item in templates:
        if 'int_col' in item or 'text_col' in item:
            fine_templates.append(item)
        else:
            coarse_templates.append(item)
        
    if os.path.exists(args.sql_config):
        sql_config = read_json(args.sql_config)
    else:
        sql_config = None
        
    # Generate data on the new database.
    if args.new_db:
        # Calculate how many tables to be generated
        table_number = args.total_number // args.each_table_number
        
        # Read the configuration of the database, column ranges, and row ranges for each table.
        database_config, column_numbers, row_numbers = \
            generate_database_config(args.database_config, args.context_length, args.tokenizer, args.context_length_format, args.db_path)
              
        
        
        ############################################## Perform pre-sampling to determine the sampling number "multiple".
        table_name = 'table_try' 
        table_path = os.path.join(args.db_path, table_name + '.db')
        
        # Random table size
        column_number = random.choice(column_numbers)
        row_number = random.choice(row_numbers)
        
        # Generate table schema
        while True:
            output = generate_table(database_config, table_path,column_number,row_number)
            if output != 0:
                break   
        
        # Insert random values into the table.
        insert_random_values(database_config, table_path, column_number, row_number)
        
        multiple = 5
        while True:
            generate_sample_number = []
            for _ in range(2):

                # Generate SQL queries
                if random_with_weight([True, False], [len(fine_templates), len(coarse_templates)]):
                    data_i = template_queries(fine_templates, args.each_table_number, table_path, sql_config, multiple=multiple, language=args.language, data_mode=args.data_mode)
                else:
                    data_i = general_queries(coarse_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
                    
                generate_sample_number.append(len(data_i))
            max_generate_samples = max(generate_sample_number)
            if max_generate_samples < args.each_table_number - 1:
                multiple = multiple * 5
                print(f"multiple: {multiple}")
            elif max_generate_samples >= args.each_table_number - 1:
                break

        delete_table(table_path)       
    
        ############################################ Generate the data formally.
        
        data = []
        i = 0
        flag = 0
        while i < table_number:
            if flag > 15:
                flag = 0
                database_config['row_min'] -= 5
                database_config['row_max'] -= 5
                row_numbers = [x - 5 for x in row_numbers]
                
            
            print(str(i) + '\n')
            table_name = 'table' + str(i)
            table_path = os.path.join(args.db_path, table_name + '.db')
            
            # Random table size
            column_number = random.choice(column_numbers)
            row_number = random.choice(row_numbers)
            
            # Generate table schema
            while True:
                output = generate_table(database_config, table_path,column_number,row_number)
                if output != 0:
                    break   
            
            # Insert random values into the table.
            insert_random_values(database_config, table_path, column_number, row_number)
            
            if args.context_length:            
                table_length = get_table_length(table_path, args.tokenizer, args.context_length_format)
                if table_length < args.context_length-0.04*args.context_length or table_length > args.context_length+0.04*args.context_length:
                    delete_table(table_path) 
                    flag += 1   
                    continue      
            
            # Generate SQL queries
            if random_with_weight([True, False], [len(fine_templates), len(coarse_templates)]):
                data_i = template_queries(fine_templates, args.each_table_number, table_path, sql_config, multiple=multiple, language=args.language, data_mode=args.data_mode)
            else:
                data_i = general_queries(coarse_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
            data.extend(data_i)
            flag = 0
            i = i + 1
    # Generate data on the exsiting database.
    else:
        from s3eval.coarse_template_real import general_queries
        # Get all tables of this database
        table_names = []
        for file_name in os.listdir(args.db_path):
            if os.path.isfile(os.path.join(args.db_path, file_name)):
                table_names.append(os.path.join(args.db_path, file_name))
                
                
        ############################################## Perform pre-sampling to determine the sampling number "multiple".
        table_path = random.choice(table_names)
        multiple = 5
        while True:
            generate_sample_number = []
            for _ in range(2):
                # Generate SQL queries
                if random_with_weight([True, False], [len(fine_templates), len(coarse_templates)]):
                    data_i = template_queries(fine_templates, args.each_table_number, table_path, sql_config, multiple=multiple, language=args.language, data_mode=args.data_mode)
                else:
                    data_i = general_queries(coarse_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
                generate_sample_number.append(len(data_i))
            max_generate_samples = max(generate_sample_number)
            if max_generate_samples < args.each_table_number - 1:
                multiple = multiple * 5
                print(f"multiple: {multiple}")
            elif max_generate_samples >= args.each_table_number - 1:
                break
        ############################################ 
                
                
        
        # Generate SQL queries
        data = []
        for i in range(args.total_number // args.each_table_number):
            print(str(i) + '\n')
            table_path = random.choice(table_names)
            
            # Generate SQL queries
            if random_with_weight([True, False], [len(fine_templates), len(coarse_templates)]):
                data_i = template_queries(fine_templates, args.each_table_number, table_path, sql_config, multiple=multiple, language=args.language, data_mode=args.data_mode)
            else:
                data_i = general_queries(coarse_templates, args.each_table_number, table_path, sql_config, multiple=multiple, data_mode=args.data_mode)
            
            data.extend(data_i)
            
    # Save generated data
    random.shuffle(data)
    print(len(data))
    if args.des == '':
        save_path = f"./data/{os.path.basename(args.db_path)}_{len(data)}_{os.path.basename(args.template).replace('.txt','').replace('.json','')}.json"
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
    parser.add_argument('--each_table_number', type=int, default=5)
    parser.add_argument('--database_config', type=str, default='./config/database_config.json')
    parser.add_argument('--sql_config', type=str, default='./config/sql_config.json')
    parser.add_argument('--template', type=str, default='./template/general.txt')
    parser.add_argument('--context_length', type=int, default=0)
    parser.add_argument('--context_length_format', choices=["markdown", "flatten"], default='flatten')
    parser.add_argument('--tokenizer', type=str, default=None)
    parser.add_argument('--data_mode', choices=['ft', 'eval'], default='eval')
    parser.add_argument('--language', type=str, default='en')
    parser.add_argument('--des', type=str, default='')
    args = parser.parse_args()

    main(args)