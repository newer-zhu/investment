"""
Email and report utilities for quantitative trading strategies
"""
import pandas as pd
from pathlib import Path
from qlib_project.utils.util import send_email, load_config_from_ini
from qlib_project.constants import CONFIG_PATH


def generate_rebound_report_html(pool_date: str, result: pd.DataFrame, total_candidates: int) -> str:
    """
    生成专业的量化报告 HTML
    """
    num_selected = len(result)

    # 格式化表格 - 添加更多有用的列
    display_cols = ['instrument', 'score', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']
    df_display = result.reset_index()[display_cols].copy()
    df_display.columns = ['股票代码', '预测得分', '5日乖离率', '20日乖离率', '量比', '振幅']

    # 格式化数值
    df_display['预测得分'] = df_display['预测得分'].round(4)
    df_display['5日乖离率'] = (df_display['5日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['20日乖离率'] = (df_display['20日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['量比'] = df_display['量比'].round(2)
    df_display['振幅'] = (df_display['振幅'] * 100).round(2).astype(str) + '%'

    # 计算统计信息
    avg_score = result['score'].mean() if not result.empty else 0
    avg_bias_5d = result['bias_5d'].mean() if not result.empty else 0
    avg_vol_ratio = result['vol_ratio'].mean() if not result.empty else 0

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
                overflow-x: auto;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 12px 8px;
                text-align: center;
                font-weight: 500;
                font-size: 12px;
                border: 1px solid #ddd;
            }}
            td {{
                padding: 10px 8px;
                text-align: center;
                border-bottom: 1px solid #ecf0f1;
                font-size: 12px;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #e8f4fd;
                transition: background-color 0.2s;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                color: #7f8c8d;
                font-size: 12px;
            }}
            .performance-metrics {{
                background: #f8f9fa;
                padding: 15px;
                border-radius: 6px;
                margin-bottom: 20px;
            }}
            .performance-metrics h3 {{
                margin-top: 0;
                color: #2c3e50;
                font-size: 16px;
            }}
            .metrics-grid {{
                display: flex;
                justify-content: space-between;
                flex-wrap: wrap;
                gap: 15px;
            }}
            .metric {{
                display: flex;
                flex-direction: column;
                align-items: center;
                background: white;
                padding: 10px;
                border-radius: 4px;
                box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                min-width: 120px;
            }}
            .metric-label {{
                font-size: 12px;
                color: #7f8c8d;
                margin-bottom: 5px;
            }}
            .metric-value {{
                font-size: 16px;
                font-weight: bold;
                color: #3498db;
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

            <div class="performance-metrics">
                <h3>📈 入选股票统计</h3>
                <div class="metrics-grid">
                    <div class="metric">
                        <span class="metric-label">平均预测得分:</span>
                        <span class="metric-value">{avg_score:.4f}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">平均5日乖离率:</span>
                        <span class="metric-value">{avg_bias_5d:.1%}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">平均量比:</span>
                        <span class="metric-value">{avg_vol_ratio:.2f}</span>
                    </div>
                </div>
            </div>

            <div class="strategy-info">
                <h3>🎯 策略逻辑说明</h3>
                <p>基于 LightGBM 模型预测未来<strong>2日</strong>最高价相对当前收盘的收益，筛选出具备超跌反弹潜力的股票。</p>
                <ul>
                    <li>✅ 5日乖离率 < -3.5%：股价位于均线下方，具备反弹基础</li>
                    <li>✅ 量比 < 1.3：缩量表明抛压衰竭，资金观望</li>
                    <li>✅ 20日乖离率控制：中期趋势未完全崩坏</li>
                    <li>✅ 振幅适中：保持基本活跃度和流动性</li>
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
            <p>Generated by Qlib Quant Strategy Engine v2.0 | {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </body>
    </html>
    """
    return html


def _render_operation_guidance(g) -> str:
    """把仓位管理生成的"明日操作指导"渲染成 HTML 区块; g 为 None 时返回空串。"""
    if not g:
        return ""

    mkt_ok = g.get("market_ok", True)
    mkt_txt = "🟢 大盘在 MA20 上方" if mkt_ok else "🔴 大盘在 MA20 下方 (风险提醒, 仅供参考)"
    if g.get("market_close") is not None:
        mkt_txt += f"　HS300 {g['market_close']:.2f} / MA20 {g['market_ma20']:.2f}"
    mkt_color = "#27ae60" if mkt_ok else "#e74c3c"

    holdings = g.get("holdings") or []
    if holdings:
        rows = ""
        for a in holdings:
            pnl = a.get("pnl")
            pnl_s = f"{pnl:+.2%}" if pnl is not None else "N/A"
            is_sell = a.get("action") == "卖出"
            color = "#e74c3c" if is_sell else "#27ae60"
            reason = a.get("reason") or ""
            trig = a.get("trigger") or ""
            rows += (
                "<tr>"
                f"<td>{a['code']}</td>"
                f"<td>{a.get('entry_date', '')}</td>"
                f"<td>{a.get('entry_price')}</td>"
                f"<td>{a.get('price') if a.get('price') is not None else '-'}</td>"
                f"<td>{pnl_s}</td>"
                f"<td>{a.get('held_days')}</td>"
                f"<td style='color:{color};font-weight:bold'>{a.get('action')}</td>"
                f"<td style='text-align:left'>{reason} {trig}</td>"
                "</tr>"
            )
        holdings_html = f"""
      <table>
        <tr><th>代码</th><th>买入日</th><th>成本</th><th>现价</th><th>盈亏</th><th>持有天</th><th>明日操作</th><th>说明 / 触发价</th></tr>
        {rows}
      </table>"""
    else:
        holdings_html = "<p style='color:#7f8c8d;'>当前无持仓。</p>"

    if g.get("buy_codes"):
        buy_note = (f"<p style='color:#27ae60;font-weight:bold;'>🟢 明日可买入候选 ({len(g['buy_codes'])}): "
                    f"{'、'.join(g['buy_codes'])}</p>"
                    f"<p style='color:#7f8c8d;font-size:12px;'>每仓等权 = 可用资金 × 0.98 / {g.get('max_positions')}; "
                    f"买入后持有 {g.get('hold_days')} 日, 止损 {g.get('stop_loss'):+.0%}, 止盈 {g.get('take_profit'):+.0%}</p>")
    elif mkt_ok:
        buy_note = "<p style='color:#7f8c8d;'>今日过滤后无符合买入条件的候选, 保持空仓/观察。</p>"
    else:
        buy_note = "<p style='color:#e74c3c;'>⚠️ 大盘在 MA20 下方 (风险提醒, 仅供参考)。</p>"

    return f"""
    <div class="summary">
      <h2>📋 明日操作指导 ({g.get('asof_date', '?')})</h2>
      <p style="font-weight:bold;color:{mkt_color};">{mkt_txt}</p>
      <h3>📦 当前持仓 ({len(holdings)} 只)</h3>
      {holdings_html}
      {buy_note}
    </div>"""


def generate_trend_report_html(pool_date: str, result: pd.DataFrame, total_candidates: int,
                               operation_guidance: dict = None) -> str:
    """
    生成趋势跟踪策略报告 HTML
    operation_guidance: 可选, 仓位管理生成的"明日操作指导" (position_manager.build_daily_guidance)。
    result: 今日买入候选; 可为 None(仅发送操作指导邮件)。
    """
    if result is None or result.empty:
        result = pd.DataFrame(
            {"score": pd.Series(dtype=float),
             "bias_5d": pd.Series(dtype=float),
             "bias_20d": pd.Series(dtype=float),
             "vol_ratio": pd.Series(dtype=float)},
            index=pd.Index([], name="instrument"),
        )
    num_selected = len(result)
    select_ratio = f"{(num_selected / total_candidates * 100):.1f}%" if total_candidates else "-"
    operation_html = _render_operation_guidance(operation_guidance)

    # 格式化表格
    display_cols = ['instrument', 'score', 'bias_5d', 'bias_20d', 'vol_ratio']
    df_display = result.reset_index()[display_cols].copy()
    df_display.columns = ['股票代码', '预测得分', '5日乖离率', '20日乖离率', '量比']

    # 格式化数值
    df_display['预测得分'] = df_display['预测得分'].round(4)
    df_display['5日乖离率'] = (df_display['5日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['20日乖离率'] = (df_display['20日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['量比'] = df_display['量比'].round(2)

    # 计算统计信息
    avg_score = result['score'].mean() if not result.empty else 0
    avg_bias_5d = result['bias_5d'].mean() if not result.empty else 0
    avg_vol_ratio = result['vol_ratio'].mean() if not result.empty else 0

    table_html = df_display.to_html(index=False, border=0, escape=False)

    # HTML 模板
    html = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>趋势跟踪策略报告</title>
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
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
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
                border-bottom: 2px solid #27ae60;
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
                color: #27ae60;
            }}
            .stat .label {{
                color: #7f8c8d;
                font-size: 14px;
                margin-top: 5px;
            }}
            .table-container {{
                background: white;
                border-radius: 8px;
                overflow-x: auto;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th {{
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                color: white;
                padding: 12px 8px;
                text-align: center;
                font-weight: 500;
                font-size: 12px;
                border: 1px solid #ddd;
            }}
            td {{
                padding: 10px 8px;
                text-align: center;
                border-bottom: 1px solid #ecf0f1;
                font-size: 12px;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #e8fdf5;
                transition: background-color 0.2s;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                color: #7f8c8d;
                font-size: 12px;
            }}
            .performance-metrics {{
                background: #f8f9fa;
                padding: 15px;
                border-radius: 6px;
                margin-bottom: 20px;
            }}
            .performance-metrics h3 {{
                margin-top: 0;
                color: #2c3e50;
                font-size: 16px;
            }}
            .metrics-grid {{
                display: flex;
                justify-content: space-between;
                flex-wrap: wrap;
                gap: 15px;
            }}
            .metric {{
                display: flex;
                flex-direction: column;
                align-items: center;
                background: white;
                padding: 10px;
                border-radius: 4px;
                box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                min-width: 120px;
            }}
            .metric-label {{
                font-size: 12px;
                color: #7f8c8d;
                margin-bottom: 5px;
            }}
            .metric-value {{
                font-size: 16px;
                font-weight: bold;
                color: #27ae60;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📈 趋势跟踪策略报告</h1>
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
                    <div class="number">{select_ratio}</div>
                    <div class="label">入选比例</div>
                </div>
            </div>

            <div class="performance-metrics">
                <h3>📈 入选股票统计</h3>
                <div class="metrics-grid">
                    <div class="metric">
                        <span class="metric-label">平均预测得分:</span>
                        <span class="metric-value">{avg_score:.4f}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">平均5日乖离率:</span>
                        <span class="metric-value">{avg_bias_5d:.1%}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">平均量比:</span>
                        <span class="metric-value">{avg_vol_ratio:.2f}</span>
                    </div>
                </div>
            </div>

            <div class="strategy-info">
                <h3>🎯 策略逻辑说明</h3>
                <p>基于 LightGBM 模型预测未来<strong>5日</strong>收益率，筛选动量延续的强势股。</p>
                <ul>
                    <li>✅ 5日乖离率 > 1%：股价位于均线上方，趋势向上</li>
                    <li>✅ 量比 < 1.5：温和放量，非异常炒作</li>
                    <li>✅ 20日乖离率辅助判断中期趋势</li>
                    <li>✅ 追涨杀跌，买已经涨的赌继续涨</li>
                </ul>
            </div>
        </div>

        {operation_html}

        <div class="table-container">
            <table>
                {table_html}
            </table>
        </div>

        <div class="footer">
            <p>⚠️ 投资有风险，入市需谨慎。本报告仅供参考，不构成投资建议。</p>
            <p>Generated by Qlib Quant Strategy Engine v2.0 | {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
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
    print(f"🔍 测试配置文件路径: {CONFIG_PATH}")
    _email_cfg = load_config_from_ini("email", str(CONFIG_PATH))

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