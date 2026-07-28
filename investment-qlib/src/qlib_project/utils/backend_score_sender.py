import json
import os
import urllib.error
import urllib.request
import traceback
import pandas as pd

BACKEND_BATCH_SAVE_URL = os.getenv("SCORES_BACKEND_URL", "http://192.168.199.139:8080/api/scores/batch-save")


def build_backend_records_from_result(result, pool_date: str):
    """Convert a prediction result DataFrame into backend payload records."""
    backend_records = []
    for instrument, row in result.iterrows():
        backend_records.append(
            {
                "tradeDate": pool_date,
                "instrument": str(instrument),
                "score": float(row["score"]),
                "bias5d": float(row["bias_5d"]),
                "volRatio": float(row["vol_ratio"]),
            }
        )
    return backend_records


def save_scores_to_backend(records, url: str = None, timeout: int = 10):
    """Send generated score records to the backend batch-save endpoint."""
    target_url = url or BACKEND_BATCH_SAVE_URL
    payload = []
    for item in records:
        if isinstance(item, dict):
            payload.append(item)
            continue

        payload.append(
            {
                "tradeDate": item.get("tradeDate"),
                "instrument": item.get("instrument"),
                "score": item.get("score"),
                "bias5d": item.get("bias5d"),
                "volRatio": item.get("volRatio"),
            }
        )

    try:
        print(f"🔗 Sending POST request to: {target_url}")
        print(f"📦 Payload size: {len(payload)} records")
        req = urllib.request.Request(
            target_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            if body:
                return json.loads(body)
            return {
                "success": True,
                "message": "Stock scores saved successfully",
                "count": len(payload),
            }

    # 🛠️ ---------- 优化后的错误打印逻辑 ----------
    except urllib.error.HTTPError as exc:
        # 当后端收到请求但返回了 400, 405, 500 等错误时触发
        error_body = exc.read().decode("utf-8")
        print(f"❌ 后端返回了 HTTP 错误码: {exc.code} {exc.reason}")
        print(f"📄 后端错误响应体: {error_body}")
        return {
            "success": False,
            "message": f"HTTP {exc.code}: {error_body}",
            "count": len(payload),
        }

    except urllib.error.URLError as exc:
        # 当网络不通、连接被拒、DNS解析失败时触发
        print(f"❌ 网络连接异常 (URLError): {exc.reason}")
        print("🔍 详细错误堆栈如下:")
        traceback.print_exc()  # 打印完整的错误链路
        return {
            "success": False,
            "message": f"URLError: {exc.reason}",
            "count": len(payload),
        }

    except Exception as exc:
        # 捕获超时、JSON解析等其他未知异常
        print(f"❌ 发生其他异常: {exc}")
        traceback.print_exc()
        return {"success": False, "message": str(exc), "count": len(payload)}

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Send stock score records to the backend batch-save endpoint."
    )
    parser.add_argument("--csv", type=str, help="Path to CSV file containing score, bias_5d, vol_ratio columns")
    parser.add_argument("--timeout", type=int, default=10, help="Request timeout in seconds")
    args = parser.parse_args()

    if args.csv:
        result = pd.read_csv(args.csv, index_col=0)
    else:
        result = pd.DataFrame(
            {
                "score": [0.283777, 0.256160, 0.193383, 0.166202, 0.165445, 0.160597],
                "bias_5d": [-0.069016, -0.035334, -0.055338, -0.046560, -0.055036, -0.057925],
                "vol_ratio": [0.701473, 0.916492, 0.602278, 0.660472, 0.532322, 0.926801],
            },
            index=[
                "SZ001896",
                "SH600396",
                "SZ002245",
                "SZ001211",
                "SZ002421",
                "SZ002613",
            ],
        )

    records = build_backend_records_from_result(result, "2026-06-30")
    backend_url = BACKEND_BATCH_SAVE_URL
    print(f"Sending {len(records)} records to {backend_url}")
    response = save_scores_to_backend(records, url=backend_url, timeout=args.timeout)
    print("Response:", response)


if __name__ == "__main__":
    main()
