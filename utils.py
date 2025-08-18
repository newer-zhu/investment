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
