from typing import Optional

def init_qlib(provider_uri: Optional[str] = None, region: str = "cn"):
    """Initialize qlib with an optional provider URI."""
    try:
        import qlib
        from qlib.config import REG_CN, REG_US, C  
    except Exception as e:
        raise RuntimeError("Please install qlib (pip install qlib) before initializing.") from e

    reg = REG_CN if region == "cn" else REG_US
    
    # 构造初始化参数
    init_kwargs = {
        "region": reg,
        "logging_level": "INFO" # 建议加上日志级别，方便调试
    }
    if provider_uri:
        init_kwargs["provider_uri"] = provider_uri

    # 执行初始化
    qlib.init(**init_kwargs)

    # --- 关键修复：绕过 Windows 中文路径编码问题 ---
    # 1. 强制设为单进程
    C["workers"] = 1  
    # 2. 强制使用串行后端，不启动任何并行池
    C["joblib_backend"] = "sequential" 
    
    print(f"Qlib initialized. Workers set to {C['workers']} to bypass encoding issues.")

def check_qlib_initialized() -> bool:
    # 注意：qlib 0.9.x+ 并没有 get_instance()，通常判断配置是否加载
    try:
        from qlib.config import C
        return "provider_uri" in C
    except Exception:
        return False