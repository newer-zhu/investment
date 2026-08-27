"""趋势策略持仓管理与每日操作指导 (与回测 backtest_trend 规则一致)。

记录你的实盘持仓, 每天基于最新收盘价 + 持仓成本, 按回测同一套规则生成"明日操作提示":
  - 卖出: 止损 / 止盈 / 趋势破坏(跌破MA{ma}×缓冲) / 持有到期 / 大盘<MA20 全仓离场
  - 买入: 复用 apply_trend_buy_filter 的候选 (由 predict_day 传入 buy_result)
规则参数统一取自 constants.TREND_RULES, 与回测 backtest_trend 保持一致。

用法 (CLI):
  python position_manager.py --mode=status [--date YYYY-MM-DD]     # 持仓 + 明日操作指导
  python position_manager.py --mode=buy  --code SZ002156 --shares 500 --price 12.34 [--date YYYY-MM-DD]
  python position_manager.py --mode=sell --code SZ002156 --shares 500 --price 13.20 [--date YYYY-MM-DD]
  python position_manager.py --mode=reset                          # 清空持仓
"""
import sys
import json
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for p in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from qlib_project.constants import TREND_POSITIONS_FILE, TREND_RULES
from qlib_project.strategy.trend_strategy import _ensure_qlib_init, _features_single_day
from qlib.data import D


# ============================================================
# 持仓状态 (JSON)
# ============================================================
def load_holdings():
    """返回持仓列表 [{code, entry_date, entry_price, shares}, ...]。"""
    if TREND_POSITIONS_FILE.exists():
        try:
            return json.loads(TREND_POSITIONS_FILE.read_text(encoding='utf-8')).get("holdings", [])
        except Exception:
            return []
    return []


def save_holdings(holdings, date_str):
    TREND_POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": date_str, "holdings": holdings}
    TREND_POSITIONS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def add_holding(code, shares, price, date_str):
    """记录一笔买入 (同代码重复买入按均价合并)。"""
    holdings = load_holdings()
    code = code.upper()
    for h in holdings:
        if h["code"] == code:
            old_sh, old_px = h["shares"], h["entry_price"]
            new_sh = old_sh + shares
            h["entry_price"] = round((old_px * old_sh + price * shares) / new_sh, 4)
            h["shares"] = new_sh
            save_holdings(holdings, date_str)
            return h
    h = {"code": code, "entry_date": date_str, "entry_price": float(price), "shares": int(shares)}
    holdings.append(h)
    save_holdings(holdings, date_str)
    return h


def sell_holding(code, shares, price, date_str):
    """记录一笔卖出 (shares 减到 0 则移除持仓)。"""
    holdings = load_holdings()
    code = code.upper()
    for h in holdings:
        if h["code"] == code:
            h["shares"] -= shares
            if h["shares"] <= 0:
                out = dict(h)
                holdings.remove(h)
            else:
                out = dict(h)
            save_holdings(holdings, date_str)
            return out
    return None


# ============================================================
# 每日操作指导
# ============================================================
def _market_status(asof_date):
    """返回 (market_ok, hs300_close, hs300_ma20); 取不到大盘数据时视为健康。"""
    mkt = D.features(["SH000300"], ["$close", "Mean($close, 20)"],
                     start_time=asof_date, end_time=asof_date)
    if mkt is None or mkt.empty:
        return True, None, None
    row = mkt.iloc[0]
    return bool(row["$close"] >= row["Mean($close, 20)"]), float(row["$close"]), float(row["Mean($close, 20)"])


def _decide_hold_action(h, price, pnl, held_days, market_ok, feats, rules):
    """按回测优先级返回 (action, reason, trigger/参考价)。"""
    # 1) 市场风控: 大盘<MA20 全仓离场 (回测 market_exit)
    if rules.get("market_exit") and not market_ok:
        return "卖出", "市场风控(大盘<MA20)", None
    # 2) 持有到期
    if held_days >= rules["hold_days"]:
        return "卖出", f"持有{rules['hold_days']}日到期", None
    # 3) 止损 / 4) 止盈
    if pnl is not None:
        if pnl <= rules["stop_loss"]:
            return "卖出", "止损", round(float(h["entry_price"]) * (1 + rules["stop_loss"]), 3)
        if pnl >= rules["take_profit"]:
            return "卖出", "止盈", round(float(h["entry_price"]) * (1 + rules["take_profit"]), 3)
    # 5) 趋势破坏: 收盘 < MA{ma}×(1-缓冲), 需持有 >= trend_break_min_hold
    code = h["code"]
    ma = rules["trend_exit_ma"]
    if rules.get("trend_exit") and held_days >= rules.get("trend_break_min_hold", 2) \
            and code in feats.index:
        bias = feats.loc[code, f"bias_{ma}d"]
        if pd.notna(bias) and bias < -rules["trend_exit_buffer"]:
            ma_now = float(feats.loc[code, "price"]) / (1 + bias)      # 反推当前 MA
            return "卖出", f"趋势破坏(跌破MA{ma})", round(ma_now * (1 - rules["trend_exit_buffer"]), 3)
    # 否则持有, 给参考触发价
    triggers = {
        "止损价": round(float(h["entry_price"]) * (1 + rules["stop_loss"]), 3),
        "止盈价": round(float(h["entry_price"]) * (1 + rules["take_profit"]), 3),
    }
    if code in feats.index:
        bias = feats.loc[code, f"bias_{ma}d"]
        if pd.notna(bias):
            ma_now = float(feats.loc[code, "price"]) / (1 + bias)
            triggers[f"趋势线MA{ma}(≈)"] = round(ma_now * (1 - rules["trend_exit_buffer"]), 3)
    trigger = " / ".join(f"{k} {v}" for k, v in triggers.items())
    return "持有", None, trigger


def build_daily_guidance(asof_date, market_ok=None, buy_result=None, rules=None):
    """生成明日操作指导 (与回测规则一致)。

    asof_date: 决策基准日 (用该日收盘价判断)。
    market_ok: 大盘是否在 MA20 上方; None 则内部探测。
    buy_result: 今日买入候选 (predict_day 的 apply_trend_buy_filter 结果, 可为 None)。
    """
    rules = rules or TREND_RULES
    _ensure_qlib_init()
    cal = D.calendar()
    asof_ts = pd.Timestamp(asof_date)

    if market_ok is None:
        market_ok, mkt_close, mkt_ma20 = _market_status(asof_ts)
    else:
        mkt_close, mkt_ma20 = None, None

    holdings = load_holdings()
    actions = []
    if holdings:
        codes = [h["code"] for h in holdings]
        feats = _features_single_day(
            codes,
            ["$close",
             "$close / Mean($close, 5) - 1",
             "$close / Mean($close, 10) - 1"],
            asof_ts,
            ["price", "bias_5d", "bias_10d"],
        )
        for h in holdings:
            code = h["code"]
            price = None
            if code in feats.index:
                price = float(feats.loc[code, "price"])
            entry = float(h["entry_price"])
            pnl = (price / entry - 1) if price else None
            # 持有天数: 按交易日计数 (回测口径: 买入次日为第1天)
            idx_entry = int(np.searchsorted(cal, pd.Timestamp(h["entry_date"])))
            idx_asof = int(np.searchsorted(cal, asof_ts))
            held_days = max(idx_asof - idx_entry, 0)

            action, reason, trigger = _decide_hold_action(
                h, price, pnl, held_days, market_ok, feats, rules)

            actions.append({
                "code": code,
                "entry_date": h.get("entry_date"),
                "entry_price": entry,
                "shares": h.get("shares"),
                "price": price,
                "pnl": pnl,
                "held_days": held_days,
                "action": action,
                "reason": reason,
                "trigger": trigger,
            })

    buy_codes = []
    if buy_result is not None and not buy_result.empty:
        buy_codes = [str(c) for c in buy_result.index]

    return {
        "asof_date": asof_ts.strftime('%Y-%m-%d'),
        "market_ok": market_ok,
        "market_close": mkt_close,
        "market_ma20": mkt_ma20,
        "holdings": actions,
        "buy_codes": buy_codes,
        "max_positions": rules["max_positions"],
        "hold_days": rules["hold_days"],
        "stop_loss": rules["stop_loss"],
        "take_profit": rules["take_profit"],
    }


# ============================================================
# CLI
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="趋势持仓管理与每日操作指导")
    parser.add_argument("--mode", choices=["status", "buy", "sell", "reset"], default="status")
    parser.add_argument("--date", type=str, default=pd.Timestamp.now().strftime('%Y-%m-%d'))
    parser.add_argument("--code", type=str)
    parser.add_argument("--shares", type=int)
    parser.add_argument("--price", type=float)
    args = parser.parse_args()

    if args.mode == "reset":
        save_holdings([], args.date)
        print("已清空持仓")
        return
    if args.mode == "buy":
        if not (args.code and args.shares and args.price):
            print("buy 需要 --code --shares --price"); return
        add_holding(args.code, args.shares, args.price, args.date)
        print(f"已买入 {args.code.upper()} {args.shares}股 @{args.price} ({args.date})")
        return
    if args.mode == "sell":
        if not (args.code and args.shares and args.price):
            print("sell 需要 --code --shares --price"); return
        out = sell_holding(args.code, args.shares, args.price, args.date)
        print(f"已卖出 {args.code.upper()} {args.shares}股 @{args.price}" if out else f"未找到持仓 {args.code}")
        return

    # status
    g = build_daily_guidance(args.date)
    print(f"决策基准日: {g['asof_date']}")
    mkt = "🟢 健康" if g["market_ok"] else "🔴 风险 (暂停买入 + 全仓离场)"
    mkt_v = f"HS300={g['market_close']} MA20={g['market_ma20']}" if g["market_close"] is not None else ""
    print(f"大盘: {mkt}  {mkt_v}")
    print(f"持仓 {len(g['holdings'])} 只:")
    for a in g["holdings"]:
        pnl_s = f"{a['pnl']:+.2%}" if a["pnl"] is not None else "N/A"
        trig = f" ({a['trigger']})" if a["trigger"] else ""
        print(f"  {a['code']} 成本{a['entry_price']} 现价{a['price']} 盈亏{pnl_s} "
              f"持有{a['held_days']}天 -> {a['action']}{(' ' + a['reason']) if a['reason'] else ''}{trig}")
    if g["buy_codes"]:
        print(f"明日可买入候选 ({len(g['buy_codes'])}): {', '.join(g['buy_codes'])}")
    else:
        print("明日无可买入候选 (大盘风控或过滤无合格)")


if __name__ == "__main__":
    main()
