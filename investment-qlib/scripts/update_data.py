import os
import subprocess
import datetime

# === 配置区 ===
TUSHARE_TOKEN = os.getenv("TUSHARE") or "fbf2b6227d7a9df2ca8e3cc18c29cd38be4929bcd77dee4b95092f4f"
OUT_DIR = "/tmp/investment_data_update"  # 临时输出
QLIB_OUTPUT = os.path.expanduser("/mnt/f/Code/investment/investment-qlib/data/source")

# 这两个是 investment_data 仓库里的脚本名
DAILY_UPDATE_SH = "daily_update.sh"
DUMP_QLIB_BIN_SH = "dump_qlib_bin.sh"


def run_shell(cmd, cwd=None):
    res = subprocess.run(cmd, shell=True, cwd=cwd)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")
    return res


def update_source():
    """
    拉最新数据入 Dolt DB（会下载最新 Tushare 数据）
    """
    print("Running daily update ...")
    env = os.environ.copy()
    env["TUSHARE"] = TUSHARE_TOKEN
    run_shell(f"bash {DAILY_UPDATE_SH}", cwd=os.getcwd())
    print("Update done.")


def export_qlib():
    """
    导出为 Qlib Binary 数据
    """
    print("Exporting Qlib .bin data ...")
    run_shell(f"bash {DUMP_QLIB_BIN_SH}", cwd=os.getcwd())
    out_tar = "qlib_bin.tar.gz"
    if not os.path.exists(out_tar):
        raise FileNotFoundError("Expected qlib_bin.tar.gz not found!")
    print("Export done. Moving to Qlib dir ...")

    os.makedirs(QLIB_OUTPUT, exist_ok=True)
    run_shell(f"tar -zxvf {out_tar} -C {QLIB_OUTPUT} --strip-components=1")
    print("Data moved to:", QLIB_OUTPUT)


def main():
    print("Start investment_data auto update:", datetime.datetime.now())
    update_source()
    export_qlib()
    print("Update complete:", datetime.datetime.now())


if __name__ == "__main__":
    main()
