import urllib.request
import tarfile
import os
from pathlib import Path

def download_qlib_data(force=False):
    url = 'https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz'
    
    # 路径配置
    project_root = Path(__file__).resolve().parents[1] # 根据你实际结构调整
    out_dir = project_root / 'data' / 'source'
    target_folder = out_dir / 'qlib_bin'
    tmp_file = out_dir / 'qlib_bin.tar.gz'

    # 1. 检查数据是否已存在
    if target_folder.exists() and not force:
        print(f"✅ 数据已存在于: {target_folder}，跳过下载。")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    # 2. 执行下载
    try:
        print(f'🚀 开始下载最新数据: {url}')
        # 使用 urlretrieve 的简单的进度回调（可选）
        def progress(block_num, block_size, total_size):
            read_so_far = block_num * block_size
            if total_size > 0:
                percent = read_so_far * 1e2 / total_size
                print(f"\r进度: {percent:5.1f}%", end="")
            else:
                print(f"\r已下载: {read_so_far / 1e6:.1f} MB", end="")

        urllib.request.urlretrieve(url, tmp_file, reporthook=progress)
        print('\n\n✅ 下载完成！')

        # 3. 解压
        print(f'📦 正在解压到: {out_dir}...')
        with tarfile.open(tmp_file, 'r:gz') as tf:
            # 过滤解压路径，防止 tar 炸弹或路径错乱
            tf.extractall(path=out_dir)
        print('✨ 解压完成')

        # 4. 清理临时文件
        if tmp_file.exists():
            tmp_file.unlink()

    except Exception as e:
        print(f'\n❌ 出错: {e}')
        if tmp_file.exists():
            print("清理残留的临时文件...")
            tmp_file.unlink()

if __name__ == "__main__":
    download_qlib_data()