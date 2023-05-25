import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def process_files(file_list, directory_path, output_df):
    schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", IntegerType(), True),
])
    for file_name in file_list:
        file_path = os.path.join(directory_path, file_name)
        temp_df = spark.read.csv(file_path, header=True, schema=schema)
        temp_df = temp_df.withColumn("Symbol", lit(os.path.splitext(os.path.basename(file_name))[0]))
        output_df = output_df.unionByName(temp_df)
    return output_df


spark = SparkSession.builder \
    .appName("DataProcessing") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


parser = argparse.ArgumentParser(description='Process stock market data files.')
parser.add_argument('directory', type=str, help='Directory path containing the data files')
parser.add_argument('number', type=int, default=2, help='Number of files to process (default: 2)')
parser.add_argument('data', type=str, choices=['etfs', 'stocks'], help='Data to process (etfs or stocks)')


args = parser.parse_args()
directory_path = args.directory
N = args.number
data = args.data

files = os.listdir(os.path.join(directory_path, data))[0:N]

# Define column type conversions
column_types = {
    "Symbol": StringType(),
    "Security Name": StringType(),
    "Date": StringType(),
    "Open": FloatType(),
    "High": FloatType(),
    "Low": FloatType(),
    "Close": FloatType(),
    "Adj Close": FloatType(),
    "Volume": IntegerType()
}

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", IntegerType(), True),
    StructField("Symbol", StringType(), True)
])

meta = spark.read.csv(os.path.join(directory_path, 'symbols_valid_meta.csv'), header=True)

df = spark.createDataFrame([], schema)

df = process_files(files, os.path.join(directory_path, data), df)

# Join with meta_df
df = df.join(meta.select("Symbol", "Security Name").withColumnRenamed("Symbol", "meta_Symbol"),
                       df.Symbol == col("meta_Symbol"), "inner").drop("meta_Symbol")

# Apply column type conversions
for column_name, column_type in column_types.items():
    df = df.withColumn(column_name, col(column_name).cast(column_type))

# Rename columns
df = df.withColumnRenamed("Security Name", "Security_Name").withColumnRenamed("Adj Close", "Adj_Close")

# Write to Parquet
df.write.parquet(os.path.join(directory_path, f'parquet_file/{data}.parquet'))
