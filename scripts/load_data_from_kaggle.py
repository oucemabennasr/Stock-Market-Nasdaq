import subprocess
import shutil
import os

# Install required packages
subprocess.call(['pip', 'install', 'kaggle', 'gwpy'])

# Create kaggle.json file
kaggle_json = '{"username":"bennasroussama","key":"9025a47c05f1866e244c283cb19d78d7"}'
with open('/.kaggle/kaggle.json', 'w') as f:
    f.write(kaggle_json)

# Set permissions for kaggle.json
subprocess.call(['chmod', '600', '/.kaggle/kaggle.json'])

# Download dataset
subprocess.call(['kaggle', 'datasets', 'download', '-d', 'jacksoncrow/stock-market-dataset'])

# Unzip dataset
shutil.unpack_archive('stock-market-dataset.zip', '/stock-market-dataset')

# Move files
os.makedirs('/stock-market-dataset', exist_ok=True)
shutil.move('symbols_valid_meta.csv', '/stock-market-dataset')
shutil.move('stocks', '/stock-market-dataset')
shutil.move('etfs', '/stock-market-dataset')
