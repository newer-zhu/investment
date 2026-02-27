
from email_job import send_daily_report_test, send_daily_report
from utils import send_email
import akshare as ak

if __name__ == "__main__":
    stock_financial_abstract_new_ths_df = ak.stock_financial_abstract_new_ths(symbol="000063", indicator="按报告期")
    print(stock_financial_abstract_new_ths_df)
    # send_daily_report()
    # send_daily_report_test()
