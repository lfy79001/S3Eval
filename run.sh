python synthetic.py \
  --db_path ../db/db1 \
  --new_db 1 \
  --total_number 1000 \
  --each_table_number  20 \
  --database_config ./config/database_config.json \
  --sql_config ./config/sql_config.json \
  --synthetic_mode general \
  --template  ./template/general.json \
  --data_mode eval 