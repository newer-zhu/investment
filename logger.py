"""
标准化的日志配置模块
提供统一的日志输出格式，支持文件和控制台输出
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(name: str = "investment", log_dir: str = "logs", level: int = logging.INFO) -> logging.Logger:
    """
    设置标准化的日志记录器
    
    Args:
        name: 日志记录器名称
        log_dir: 日志文件目录
        level: 日志级别，默认INFO
    
    Returns:
        配置好的logger实例
    """
    logger = logging.getLogger(name)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # 创建日志目录
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # 统一的日志格式
    # 格式: [时间] [级别] [模块] 消息
    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台输出handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件输出handler（按日期分割）
    today = datetime.now().strftime("%Y%m%d")
    log_file = log_path / f"investment_{today}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # 文件记录更详细的信息
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


# 创建全局logger实例
logger = setup_logger()

