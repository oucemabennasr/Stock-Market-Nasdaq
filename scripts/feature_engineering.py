import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


parser = argparse.ArgumentParser(description='Process stock market data files.')
parser.add_argument('directory', type=str, help='Directory path containing the data parquet files')
parser.add_argument('data', type=str, choices=['etfs', 'stocks'], help='Data to process (etfs or stocks)')

args = parser.parse_args()
directory_path = args.directory
data = args.data

spark = SparkSession.builder.appName("FeatureEngineering")\
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


window_spec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)


@pandas_udf("float", PandasUDFType.GROUPED_AGG)
def calculate_median(arr):
    arr_copy = arr.copy()
    return arr_copy.median()


df = spark.read.parquet(os.path.join(directory_path, f'{data}.parquet')
                        
df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
df = df.withColumn("vol_moving_avg", avg("Volume").over(window_spec))
df = df.withColumn("adj_close_rolling_med", calculate_median(col("Adj_Close")).over(window_spec))
df = df.withColumn("Date", col("Date").cast(StringType()))

df.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/FeatureEngineeringData/etfs.parquet")
