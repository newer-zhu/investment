#!/usr/bin/env python3
"""
测试优化后的选股系统
验证各项功能是否正常工作
"""

import time
import logging
from selectStocks import (
    init_quote_dict, init_fund_flow_cache, init_half_year_high, 
    load_ljqd_blacklist, check_market_conditions, adjust_strategy_for_market,
    calculate_technical_score, calculate_fundamental_score, 
    calculate_momentum_score, calculate_risk_score
)
from chinese_market_utils import (
    get_market_sentiment, get_sector_rotation, calculate_market_timing_score
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_market_analysis():
    """测试市场分析功能"""
    logger.info("=== 测试市场分析功能 ===")
    
    try:
        # 测试市场环境检测
        market_conditions = check_market_conditions()
        logger.info(f"市场环境: {market_conditions}")
        
        # 测试策略调整
        strategy_params = adjust_strategy_for_market(market_conditions)
        logger.info(f"策略参数: {strategy_params}")
        
        # 测试市场情绪
        sentiment = get_market_sentiment()
        logger.info(f"市场情绪: {sentiment}")
        
        # 测试行业轮动
        sector_rotation = get_sector_rotation()
        logger.info(f"行业轮动 - 强势行业: {sector_rotation.get('top_sectors', [])[:3]}")
        
        # 测试择时评分
        timing_score = calculate_market_timing_score()
        logger.info(f"市场择时评分: {timing_score}")
        
        return True
    except Exception as e:
        logger.error(f"市场分析测试失败: {e}")
        return False

def test_data_initialization():
    """测试数据初始化功能"""
    logger.info("=== 测试数据初始化功能 ===")
    
    try:
        start_time = time.time()
        
        # 初始化行情数据
        logger.info("初始化行情数据...")
        init_quote_dict()
        
        # 初始化资金流数据
        logger.info("初始化资金流数据...")
        init_fund_flow_cache()
        
        # 初始化新高数据
        logger.info("初始化新高数据...")
        init_half_year_high()
        
        # 初始化黑名单
        logger.info("初始化黑名单...")
        load_ljqd_blacklist()
        
        end_time = time.time()
        logger.info(f"数据初始化完成，耗时: {end_time - start_time:.2f}秒")
        
        return True
    except Exception as e:
        logger.error(f"数据初始化测试失败: {e}")
        return False

def test_scoring_functions():
    """测试评分函数"""
    logger.info("=== 测试评分函数 ===")
    
    # 测试股票代码（以平安银行为例）
    test_code = "000001"
    
    try:
        # 测试技术评分
        tech_score = calculate_technical_score(test_code)
        logger.info(f"技术评分 ({test_code}): {tech_score}")
        
        # 测试基本面评分
        fundamental_score = calculate_fundamental_score(test_code, "银行")
        logger.info(f"基本面评分 ({test_code}): {fundamental_score}")
        
        # 测试动量评分（需要行情数据）
        from selectStocks import QUOTE_DICT
        if test_code in QUOTE_DICT:
            momentum_score = calculate_momentum_score(test_code, QUOTE_DICT[test_code])
            logger.info(f"动量评分 ({test_code}): {momentum_score}")
            
            # 测试风险评分
            from selectStocks import get_fundamental_data
            fundamental_data = get_fundamental_data(test_code, "银行")
            risk_score = calculate_risk_score(test_code, QUOTE_DICT[test_code], fundamental_data)
            logger.info(f"风险评分 ({test_code}): {risk_score}")
        
        return True
    except Exception as e:
        logger.error(f"评分函数测试失败: {e}")
        return False

def test_performance():
    """测试性能优化"""
    logger.info("=== 测试性能优化 ===")
    
    try:
        # 测试缓存效果
        start_time = time.time()
        
        # 第一次计算技术指标
        tech_score1 = calculate_technical_score("000001")
        first_time = time.time() - start_time
        
        # 第二次计算（应该使用缓存）
        start_time = time.time()
        tech_score2 = calculate_technical_score("000001")
        second_time = time.time() - start_time
        
        logger.info(f"首次计算耗时: {first_time:.3f}秒")
        logger.info(f"缓存计算耗时: {second_time:.3f}秒")
        logger.info(f"性能提升: {(first_time - second_time) / first_time * 100:.1f}%")
        
        return True
    except Exception as e:
        logger.error(f"性能测试失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("开始测试优化后的选股系统...")
    
    tests = [
        ("市场分析功能", test_market_analysis),
        ("数据初始化", test_data_initialization),
        ("评分函数", test_scoring_functions),
        ("性能优化", test_performance),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n开始测试: {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
            if result:
                logger.info(f"✓ {test_name} 测试通过")
            else:
                logger.error(f"✗ {test_name} 测试失败")
        except Exception as e:
            logger.error(f"✗ {test_name} 测试异常: {e}")
            results.append((test_name, False))
    
    # 输出测试结果汇总
    logger.info("\n=== 测试结果汇总 ===")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "通过" if result else "失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总体结果: {passed}/{total} 项测试通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！系统优化成功！")
    else:
        logger.warning("⚠️ 部分测试失败，请检查相关功能")

if __name__ == "__main__":
    main()
