import urllib.request, tarfile
from pathlib import Path

url = 'https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz'
out_dir = Path('data/source')
out_dir.mkdir(parents=True, exist_ok=True)
print('Downloading', url)
tmp = Path('tmp_qlib_bin.tar.gz')
try:
    urllib.request.urlretrieve(url, tmp)
    print('Downloaded to', tmp)
    print('Extracting to', out_dir)
    with tarfile.open(tmp, 'r:gz') as tf:
        tf.extractall(path=out_dir)
    print('Extraction complete')
    tmp.unlink()
except Exception as e:
    print('Failed:', e)
