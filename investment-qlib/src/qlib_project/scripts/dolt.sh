dolt sql -q "SHOW TABLES;"
dolt sql -q "SELECT * FROM ts_a_stock_eod_price order by tradedate desc LIMIT 10;"

cd /dolt/investment_data/ 
dolt add stock_score 
dolt commit -m "daily" 

cd /investment_data/ 
bash dump_qlib_bin.sh /
cd /dolt/investment_data/ 
dolt sql-server --host 0.0.0.0 --port 3306 --allow-cleartext-passwords=true &

ps -ef | grep "dolt sql-serer" 
