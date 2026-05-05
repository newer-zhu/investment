"""
Email and report utilities for quantitative trading strategies
"""
import pandas as pd
from pathlib import Path
from qlib_project.utils.util import send_email, load_config_from_ini


def generate_rebound_report_html(pool_date: str, result: pd.DataFrame, total_candidates: int) -> str:
    """
    生成专业的量化报告 HTML
    """
    num_selected = len(result)

    # 格式化表格
    df_display = result.reset_index()[['instrument', 'score', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']].copy()
    df_display.columns = ['股票代码', '预测得分', '5日乖离率', '20日乖离率', '量比', '振幅']

    # 格式化数值
    df_display['预测得分'] = df_display['预测得分'].round(4)
    df_display['5日乖离率'] = (df_display['5日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['20日乖离率'] = (df_display['20日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['量比'] = df_display['量比'].round(2)
    df_display['振幅'] = (df_display['振幅'] * 100).round(2).astype(str) + '%'

    table_html = df_display.to_html(index=False, border=0, escape=False)

    # HTML 模板
    html = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>超跌反弹策略报告</title>
        <style>
            body {{
                font-family: 'Microsoft YaHei', 'PingFang SC', 'Hiragino Sans GB', sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f8f9fa;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                text-align: center;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
                font-weight: 300;
            }}
            .header p {{
                margin: 10px 0 0 0;
                opacity: 0.9;
            }}
            .summary {{
                background: white;
                padding: 25px;
                border-radius: 8px;
                margin-bottom: 25px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            }}
            .summary h2 {{
                color: #2c3e50;
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
                margin-top: 0;
            }}
            .stats {{
                display: flex;
                justify-content: space-around;
                margin: 20px 0;
            }}
            .stat {{
                text-align: center;
            }}
            .stat .number {{
                font-size: 32px;
                font-weight: bold;
                color: #3498db;
            }}
            .stat .label {{
                color: #7f8c8d;
                font-size: 14px;
                margin-top: 5px;
            }}
            .table-container {{
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th {{
                background: #34495e;
                color: white;
                padding: 15px 10px;
                text-align: center;
                font-weight: 500;
                font-size: 14px;
            }}
            td {{
                padding: 12px 10px;
                text-align: center;
                border-bottom: 1px solid #ecf0f1;
                font-size: 13px;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #e8f4fd;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                color: #7f8c8d;
                font-size: 12px;
            }}
            .strategy-info {{
                background: #ecf0f1;
                padding: 15px;
                border-radius: 6px;
                margin-bottom: 20px;
            }}
            .strategy-info h3 {{
                margin-top: 0;
                color: #2c3e50;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 超跌反弹策略报告</h1>
            <p>报告日期：{pool_date} | 生成时间：{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <div class="summary">
            <h2>📊 策略执行概况</h2>
            <div class="stats">
                <div class="stat">
                    <div class="number">{total_candidates}</div>
                    <div class="label">候选股票数</div>
                </div>
                <div class="stat">
                    <div class="number">{num_selected}</div>
                    <div class="label">入选股票数</div>
                </div>
                <div class="stat">
                    <div class="number">{(num_selected/total_candidates*100):.1f}%</div>
                    <div class="label">入选比例</div>
                </div>
            </div>

            <div class="strategy-info">
                <h3>🎯 策略逻辑说明</h3>
                <p>基于 LightGBM 模型预测未来2日最高价相对当前收盘的收益，筛选出具备超跌反弹潜力的股票。</p>
                <ul>
                    <li>✅ 5日乖离率 < 0：股价位于均线下方</li>
                    <li>✅ 量比 < 1.1：缩量表明抛压衰竭</li>
                    <li>✅ 20日乖离率 > -15%：中期趋势未完全崩坏</li>
                    <li>✅ 振幅 > 1.5%：保持基本活跃度</li>
                </ul>
            </div>
        </div>

        <div class="table-container">
            <table>
                {table_html}
            </table>
        </div>

        <div class="footer">
            <p>⚠️ 投资有风险，入市需谨慎。本报告仅供参考，不构成投资建议。</p>
            <p>Generated by Qlib Quant Strategy Engine</p>
        </div>
    </body>
    </html>
    """
    return html


def send_rebound_strategy_report(pool_date: str, result: pd.DataFrame, total_candidates: int, config_path: str):
    """
    发送超跌反弹策略报告邮件

    Args:
        pool_date: 报告日期
        result: 选股结果DataFrame
        total_candidates: 候选股票总数
        config_path: 配置文件路径
    """
    try:
        print(f"🔍 配置文件路径: {config_path}")
        _email_cfg = load_config_from_ini("email", str(config_path))
        TO_EMAILS = [e.strip() for e in _email_cfg.get("to_emails", "").split(",") if e.strip()] or [_email_cfg.get("to_email", "")]
        FROM_EMAIL = _email_cfg.get("from_email", "")
        FROM_PASSWORD = _email_cfg.get("from_password", "")
        SMTP_SERVER = _email_cfg.get("smtp_server", "smtp.qq.com")
        SMTP_PORT = int(_email_cfg.get("smtp_port", "587"))

        print(f"📧 邮件配置 - 发件人: {FROM_EMAIL}")
        print(f"📧 邮件配置 - 密码: {FROM_PASSWORD[:4]}****")  # 只显示前4位
        print(f"📧 邮件配置 - 收件人: {TO_EMAILS}")
        print(f"📧 邮件配置 - SMTP: {SMTP_SERVER}:{SMTP_PORT}")

        # 生成 HTML 报告
        html_body = generate_rebound_report_html(pool_date, result, total_candidates)

        subject = f"🚀 超跌反弹策略报告 - {pool_date}"

        for to_email in TO_EMAILS:
            send_email(
                subject=subject,
                body=html_body,
                to_email=to_email,
                from_email=FROM_EMAIL,
                from_password=FROM_PASSWORD,
                smtp_server=SMTP_SERVER,
                smtp_port=SMTP_PORT,
                content_type="html"
            )
        print("📧 邮件报告已发送成功！")
        return True
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")
        return False


def test_send_email_functionality(project_root: Path):
    """
    测试邮件发送功能，使用 rebound_picks_2026-04-27.csv 作为示例数据
    """
    # 测试日期
    test_date = "2026-04-27"

    # 加载测试数据
    csv_path = project_root / "data" / "rebound_predictions" / f"rebound_picks_{test_date}.csv"
    if not csv_path.exists():
        print(f"❌ 测试数据文件不存在: {csv_path}")
        return

    print(f"📂 加载测试数据: {csv_path}")
    test_data = pd.read_csv(csv_path)

    # 加载邮件配置
    config_path = project_root.parent.parent.parent / "config.ini"  # /mnt/f/Code/investment/config.ini
    print(f"🔍 测试配置文件路径: {config_path}")
    _email_cfg = load_config_from_ini("email", str(config_path))

    print(f"📧 测试邮件配置 - 发件人: {_email_cfg.get('from_email', '')}")
    print(f"📧 测试邮件配置 - 密码: {_email_cfg.get('from_password', '')[:4]}****")
    print(f"📧 测试邮件配置 - SMTP: {_email_cfg.get('smtp_server', 'smtp.qq.com')}:{_email_cfg.get('smtp_port', '465')}")

    # 生成HTML报告
    html_body = generate_rebound_report_html(test_date, test_data, 150)  # 假设150个候选股票

    # 邮件参数
    subject = f"🚀 超跌反弹策略报告 - {test_date} (测试邮件)"
    to_emails = [e.strip() for e in _email_cfg.get("to_emails", "").split(",") if e.strip()] or [_email_cfg.get("to_email", "")]

    print(f"📧 测试发送邮件到: {', '.join(to_emails)}")
    print(f"📊 报告包含 {len(test_data)} 只股票")

    # 发送测试邮件
    success_count = 0
    for to_email in to_emails:
        try:
            send_email(
                subject=subject,
                body=html_body,
                to_email=to_email,
                from_email=_email_cfg.get("from_email", ""),
                from_password=_email_cfg.get("from_password", ""),
                smtp_server=_email_cfg.get("smtp_server", "smtp.qq.com"),
                smtp_port=int(_email_cfg.get("smtp_port", "465")),
                content_type="html"
            )
            print(f"✅ 邮件发送成功: {to_email}")
            success_count += 1
        except Exception as e:
            print(f"❌ 邮件发送失败 {to_email}: {e}")

    print(f"\n📈 测试完成: {success_count}/{len(to_emails)} 封邮件发送成功")