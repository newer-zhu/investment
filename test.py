
from utils import send_email

if __name__ == "__main__":
    send_email(
        subject="股票提醒",
        body="你关注的股票满足条件了！",
        to_email="1713622254@qq.com",
        from_email="1713622254@qq.com",
        from_password="xjahgydqxuqtbjac",  # 邮箱授权码
        smtp_server="smtp.qq.com",  # 如果你用QQ邮箱
        smtp_port=587
    )
