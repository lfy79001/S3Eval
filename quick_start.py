import json
from s3eval.S3Eval import S3Eval
from s3eval.value_utils import read_json

"""
If you want to reproduce the experiments in the paper, please use
"""
s3eval = S3Eval("general")  # general, easy, long2k, long4k, long8k, long16k
output_path = "./data/general1.json"
data = s3eval.generate_data(500, output_path)

