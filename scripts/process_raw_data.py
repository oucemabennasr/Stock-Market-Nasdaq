import os
import pyspark
#import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def process_files(file_list, directory_path, output_df):
    for file_name in file_list:
        file_path = os.path.join(directory_path, file_name)
        temp_df = spark.read.csv(file_path, header=True, inferSchema=True)
        temp_df = temp_df.withColumn("Symbol", lit(os.path.splitext(os.path.basename(file_name))[0]))
        output_df = output_df.unionByName(temp_df)
    return output_df

spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

meta_df = spark.read.csv('/home/cloud_user/Stock-Market-Nasdaq/data/symbols_valid_meta.csv', header=True)

file_etfs = os.listdir('/home/cloud_user/Stock-Market-Nasdaq/data/etfs')[0:50]
file_stocks = os.listdir('/home/cloud_user/Stock-Market-Nasdaq/data/stocks')[0:50]

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Adj Close", FloatType(), True),
    StructField("Volume", FloatType(), True),
    StructField("Symbol", StringType(), True)
])

df_etfs = spark.createDataFrame([], schema)
df_stocks = spark.createDataFrame([], schema)

# Create separate threads for ETFs and stocks file processing
#etfs_thread = threading.Thread(target=process_files, args=(file_etfs, '/home/cloud_user/Stock-Market-Nasdaq/data/etfs', df_etfs))
#stocks_thread = threading.Thread(target=process_files, args=(file_stocks, '/home/cloud_user/Stock-Market-Nasdaq/data/stocks', df_stocks))

# Start the threads
#etfs_thread.start()
#stocks_thread.start()

# Wait for the threads to finish
#etfs_thread.join()
#stocks_thread.join()

df_etfs = process_files(file_etfs, '/home/cloud_user/Stock-Market-Nasdaq/data/etfs', df_etfs)

df_stocks = process_files(file_stocks, '/home/cloud_user/Stock-Market-Nasdaq/data/stocks', df_etfs)

# Join with meta_df
df_etfs = df_etfs.join(meta_df.select("Symbol", "Security Name").withColumnRenamed("Symbol", "meta_Symbol"),
                       df_etfs.Symbol == col("meta_Symbol"), "inner").drop("meta_Symbol")

df_stocks = df_stocks.join(meta_df.select("Symbol", "Security Name").withColumnRenamed("Symbol", "meta_Symbol"),
                           df_stocks.Symbol == col("meta_Symbol"), "inner").drop("meta_Symbol")

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
    "Volume": FloatType()
}

# Apply column type conversions in a loop
for column_name, column_type in column_types.items():
    df_etfs = df_etfs.withColumn(column_name, col(column_name).cast(column_type))
    df_stocks = df_stocks.withColumn(column_name, col(column_name).cast(column_type))

# Rename columns
df_etfs = df_etfs.withColumnRenamed("Security Name", "Security_Name").withColumnRenamed("Adj Close", "Adj_Close")
df_stocks = df_stocks.withColumnRenamed("Security Name", "Security_Name").withColumnRenamed("Adj Close", "Adj_Close")

# Write to Parquet
df_etfs.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/data/parquet_file/etfs.parquet")
df_stocks.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/data/parquet_file/stocks.parquet")
