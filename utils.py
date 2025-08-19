# utils.py

def parse_number(s):
    if s is None:
        return 0.0
    if isinstance(s, float) or isinstance(s, int):
        return float(s)
    if not isinstance(s, str):
        s = str(s)
    s = s.strip().replace(",", "")
    
    try:
        if s.endswith("%"):
            return float(s.replace("%", "")) / 100.0
        if "万" in s or "亿" in s:
            s = s.replace("万", "*1e4").replace("亿", "*1e8")
            return float(eval(s))
        return float(s)
    except:
        return 0.0


def safe_get(df, field):
    val = df.get(field)
    if val is None:
        return 0
    return val

def is_industry(industry: str, keywords: list[str]) -> bool:
    """
    判断行业是否属于给定的关键词列表（模糊匹配）

    :param industry: 行业名称（字符串）
    :param keywords: 关键词列表，例如 ["科技", "半导体", "新能源"]
    :return: True 如果行业名称包含任一关键词，否则 False
    """
    if not industry:
        return False
    return any(k in industry for k in keywords)
