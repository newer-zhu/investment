bash daily_update.sh &&
bash dump_qlib_bin.sh &&
cp ./qlib_bin.tar.gz /output/

tar -zxvf investment-qlib/data/source/qlib_bin.tar.gz -C investment-qlib/data/source/ --strip-components=1