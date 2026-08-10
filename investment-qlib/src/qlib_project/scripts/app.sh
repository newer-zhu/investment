bash daily_update.sh &&
bash dump_qlib_bin.sh / --finance

bash update_qlib_bin.sh 


python3 /qlib/scripts/dump_bin.py dump_all  \
   --data_path /investment_data/qlib/qlib_normalize   \
     --qlib_dir  /output/qlib_bin/   \
       --date_field_name=tradedate   \
         --exclude_fields=tradedate,symbol,end_date

/home/holden/miniconda3/envs/qlib/bin/python /mnt/f/Code/investment/investment-qlib/src/qlib_project/strategy/rebound_strategy.py --mode=predict --date=2026-06-17


# 每周一训练一次
python trend_strategy.py --mode=train

# 每天预测（自动用最新交易日）
python trend_strategy.py --mode=predict

# 指定日期
python trend_strategy.py --mode=predict --date=2026-08-04