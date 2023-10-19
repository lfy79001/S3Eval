from S3Eval import S3Eval


s3eval = S3Eval("general")
output_path = "./data/general1.json"
data = s3eval.generate_data(500, output_path)