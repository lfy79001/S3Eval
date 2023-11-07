import json
from s3eval.S3Eval import S3Eval
from s3eval.value_utils import read_json

"""
If you want to reproduce the experiments in the paper, please use

s3eval = S3Eval("general")  # general, easy, long2k, long4k, long8k, long16k
output_path = "./data/general1.json"
data = s3eval.generate_data(500, output_path)

"""


# """
# If you want to quickly generate data for a specific number of tokens
# """
# s3eval = S3Eval("custom")

# template = "./template/general.json"
# database_config = read_json("./config/database_config.json")
# sql_config = read_json("./config/sql_config.json")

# s3eval.set_template(template)
# s3eval.set_database_config(database_config)
# s3eval.set_sql_config(sql_config)


# # if you want to generate table with specific number of tokens
# s3eval.set_context_length(1000)
# s3eval.set_context_length_format("flatten")
# s3eval.set_tokenizer("")


# output_path = "./data/general1.json"
# data = s3eval.generate_data(500, output_path) 