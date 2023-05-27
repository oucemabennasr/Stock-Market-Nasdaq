#!/bin/bash

# Install required packages
sudo apt-get install pip
pip install kaggle

# Create kaggle.json file
kaggle_json='{"username":"bennasroussama","key":"9025a47c05f1866e244c283cb19d78d7"}'
export KAGGLE_USERNAME=bennasroussama
export KAGGLE_KEY=9025a47c05f1866e244c283cb19d78d7
mkdir  ~/.kaggle
echo "$kaggle_json" > ~/.kaggle/kaggle.json

# Set permissions for kaggle.json
#chmod 600 ~/.kaggle/kaggle.json

# Download dataset
kaggle datasets download -d jacksoncrow/stock-market-dataset

# Unzip dataset
dataset_zip="stock-market-dataset.zip"
output_dir="/stock-market-dataset"  # Replace with the desired destination directory
mkdir  "$output_dir"
unzip "$dataset_zip" -d "$output_dir"

# Move files
mv symbols_valid_meta.csv stocks etfs "$output_dir"
