bash daily_update.sh &&
bash dump_qlib_bin.sh 

bash update_qlib_bin.sh 

python3 /qlib/scripts/dump_bin.py dump_all \
    --data_path /fundamental \
    --qlib_dir  /qlib_fundamental \
    --date_field_name=date \
    --exclude_fields=date,symbol,end_date