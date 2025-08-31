"""
中国股市专用工具函数
针对A股市场的特殊性和量化交易需求
"""

import akshare as ak
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def get_stock_industry_category(industry: str) -> str:
    """
    将股票行业分类为投资风格类别
    """
    if not industry:
        return "其他"
    
    # 成长股
    growth_industries = ["科技", "半导体", "互联网", "新能源", "软件", "芯片", "AI", "通信", "生物医药", "医疗器械"]
    # 价值股
    value_industries = ["银行", "保险", "房地产", "建筑", "钢铁", "煤炭", "石油", "化工", "电力", "公用事业"]
    # 消费股
    consumer_industries = ["食品饮料", "家电", "汽车", "服装", "零售", "旅游", "传媒", "教育"]
    # 周期股
    cyclical_industries = ["有色", "金属", "建材", "机械", "船舶", "航空", "航运"]
    
    if any(keyword in industry for keyword in growth_industries):
        return "成长股"
    elif any(keyword in industry for keyword in value_industries):
        return "价值股"
    elif any(keyword in industry for keyword in consumer_industries):
        return "消费股"
    elif any(keyword in industry for keyword in cyclical_industries):
        return "周期股"
    else:
        return "其他"

def calculate_chinese_market_indicators(code: str) -> Dict:
    """
    计算A股特有的技术指标
    """
    try:
        # 获取K线数据
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if len(df) < 60:
            return {}
        
        close = df['收盘'].values
        high = df['最高'].values
        low = df['最低'].values
        volume = df['成交量'].values
        
        # 计算技术指标
        indicators = {}
        
        # 1. 量价关系指标
        price_volume_ratio = np.corrcoef(close[-20:], volume[-20:])[0, 1] if len(close) >= 20 else 0
        indicators['price_volume_corr'] = price_volume_ratio
        
        # 2. 振幅指标
        amplitude = (high - low) / close * 100
        indicators['avg_amplitude'] = np.mean(amplitude[-20:])
        
        # 3. 连涨连跌天数
        price_changes = np.diff(close)
        consecutive_up = 0
        consecutive_down = 0
        
        for i in range(len(price_changes) - 1, -1, -1):
            if price_changes[i] > 0:
                consecutive_up += 1
            else:
                break
                
        for i in range(len(price_changes) - 1, -1, -1):
            if price_changes[i] < 0:
                consecutive_down += 1
            else:
                break
        
        indicators['consecutive_up'] = consecutive_up
        indicators['consecutive_down'] = consecutive_down
        
        # 4. 突破强度
        ma20 = pd.Series(close).rolling(20).mean().iloc[-1]
        ma60 = pd.Series(close).rolling(60).mean().iloc[-1]
        current_price = close[-1]
        
        if current_price > ma20 and ma20 > ma60:
            indicators['breakout_strength'] = (current_price - ma20) / ma20
        else:
            indicators['breakout_strength'] = 0
        
        # 5. 成交量放大倍数
        volume_ma5 = pd.Series(volume).rolling(5).mean().iloc[-1]
        volume_ma20 = pd.Series(volume).rolling(20).mean().iloc[-1]
        indicators['volume_ratio_5'] = volume[-1] / volume_ma5 if volume_ma5 > 0 else 1
        indicators['volume_ratio_20'] = volume[-1] / volume_ma20 if volume_ma20 > 0 else 1
        
        return indicators
        
    except Exception as e:
        logger.warning(f"计算技术指标失败 {code}: {e}")
        return {}

def get_market_sentiment() -> Dict:
    """
    获取市场情绪指标
    """
    try:
        # 获取涨跌停数据
        limit_up = ak.stock_zh_a_spot_em()
        limit_up_count = len(limit_up[limit_up['涨跌幅'] >= 9.8])
        limit_down_count = len(limit_up[limit_up['涨跌幅'] <= -9.8])
        
        # 获取北向资金
        try:
            north_flow = ak.stock_hsgt_north_net_flow_in()
            today_north_flow = north_flow.iloc[0]['value'] if not north_flow.empty else 0
        except:
            today_north_flow = 0
        
        # 计算市场情绪
        total_stocks = len(limit_up)
        up_count = len(limit_up[limit_up['涨跌幅'] > 0])
        down_count = len(limit_up[limit_up['涨跌幅'] < 0])
        
        sentiment = {
            'limit_up_count': limit_up_count,
            'limit_down_count': limit_down_count,
            'up_down_ratio': up_count / down_count if down_count > 0 else 1,
            'north_flow': today_north_flow,
            'market_strength': (up_count - down_count) / total_stocks if total_stocks > 0 else 0
        }
        
        return sentiment
        
    except Exception as e:
        logger.warning(f"获取市场情绪失败: {e}")
        return {}

def get_sector_rotation() -> Dict:
    """
    获取行业轮动信息
    """
    try:
        # 获取行业涨跌幅
        sector_data = ak.stock_board_industry_name_em()
        
        # 计算行业强度
        sector_strength = {}
        for _, row in sector_data.iterrows():
            sector_name = row['板块名称']
            change_pct = row['涨跌幅']
            sector_strength[sector_name] = change_pct
        
        # 排序找出强势行业
        sorted_sectors = sorted(sector_strength.items(), key=lambda x: x[1], reverse=True)
        
        return {
            'top_sectors': sorted_sectors[:5],
            'bottom_sectors': sorted_sectors[-5:],
            'sector_strength': sector_strength
        }
        
    except Exception as e:
        logger.warning(f"获取行业轮动失败: {e}")
        return {}

def calculate_risk_adjusted_return(code: str, days: int = 60) -> float:
    """
    计算风险调整后收益（夏普比率简化版）
    """
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if len(df) < days:
            return 0
        
        # 计算日收益率
        returns = df['收盘'].pct_change().dropna()
        if len(returns) < days:
            return 0
        
        recent_returns = returns.tail(days)
        
        # 计算年化收益率和波动率
        avg_return = recent_returns.mean() * 252  # 年化
        volatility = recent_returns.std() * np.sqrt(252)  # 年化波动率
        
        if volatility == 0:
            return 0
        
        # 夏普比率（假设无风险利率为3%）
        risk_free_rate = 0.03
        sharpe_ratio = (avg_return - risk_free_rate) / volatility
        
        return sharpe_ratio
        
    except Exception as e:
        logger.warning(f"计算风险调整收益失败 {code}: {e}")
        return 0

def get_stock_news_sentiment(code: str) -> Dict:
    """
    获取股票相关新闻情绪（简化版）
    """
    try:
        # 这里可以接入新闻API，暂时返回模拟数据
        # 实际应用中可以使用百度新闻、新浪财经等API
        
        return {
            'news_count': 0,
            'sentiment_score': 0.5,  # 0-1，0.5为中性
            'hot_topics': []
        }
        
    except Exception as e:
        logger.warning(f"获取新闻情绪失败 {code}: {e}")
        return {}

def calculate_market_timing_score() -> float:
    """
    计算市场择时评分
    """
    try:
        # 获取上证指数数据
        sh_index = ak.stock_zh_index_spot()
        sh_data = sh_index[sh_index['代码'] == '000001'].iloc[0]
        
        # 获取市场情绪
        sentiment = get_market_sentiment()
        
        # 计算择时评分
        score = 50  # 基础分
        
        # 指数趋势
        sh_change = sh_data.get('涨跌幅', 0)
        if sh_change > 0.02:
            score += 20
        elif sh_change > 0:
            score += 10
        elif sh_change < -0.02:
            score -= 20
        
        # 市场情绪
        up_down_ratio = sentiment.get('up_down_ratio', 1)
        if up_down_ratio > 2:
            score += 15
        elif up_down_ratio > 1.5:
            score += 10
        elif up_down_ratio < 0.5:
            score -= 15
        
        # 北向资金
        north_flow = sentiment.get('north_flow', 0)
        if north_flow > 10e8:  # 净流入超过10亿
            score += 15
        elif north_flow < -10e8:  # 净流出超过10亿
            score -= 15
        
        return max(0, min(100, score))
        
    except Exception as e:
        logger.warning(f"计算择时评分失败: {e}")
        return 50
