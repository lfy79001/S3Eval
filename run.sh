python synthetic.py \
  --db_path ./db/db1 \
  --new_db 1 \
  --total_number 200 \
  --each_table_number  5 \
  --database_config ./config/database_config.json \
  --sql_config ./config/sql_config.json \
  --template  ./template/general.txt 



# python synthetic.py \
#   --db_path ./db/db1 \
#   --new_db 1 \
#   --total_number 400 \
#   --each_table_number  20 \
#   --database_config ./config/database_config.json \
#   --sql_config ./config/sql_config.json \
#   --template  ./template/easy.txt \
#   --context_length 1000 \
#   --context_length_format flatten \
#   --tokenizer mistralai/Mistral-7B-v0.1