dolt sql -q "SHOW TABLES;"
dolt sql -q "SELECT * FROM ts_a_stock_eod_price order by tradedate desc LIMIT 10;"
