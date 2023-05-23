import pyspark
#import pyarrow as pa
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, median
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


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

df_etfs = spark.read.parquet("/home/cloud_user/Stock-Market-Nasdaq/data/parquet_file/etfs.parquet")
df_stocks = spark.read.parquet("/home/cloud_user/Stock-Market-Nasdaq/data/parquet_file/stocks.parquet")


df_etfs = df_etfs.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
df_stocks = df_stocks.withColumn("Date", to_date("Date", "yyyy-MM-dd"))


df_etfs = df_etfs.withColumn("vol_moving_avg", avg("Volume").over(window_spec))
df_stocks = df_stocks.withColumn("vol_moving_avg", avg("Volume").over(window_spec))


df_etfs = df_etfs.withColumn("adj_close_rolling_med", calculate_median(col("Adj_Close")).over(window_spec))
df_stocks = df_stocks.withColumn("adj_close_rolling_med", calculate_median(col("Adj_Close")).over(window_spec))


df_etfs = df_etfs.withColumn("Date", col("Date").cast(StringType()))
df_stocks = df_stocks.withColumn("Date", col("Date").cast(StringType()))

df_etfs.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/FeatureEngineeringData/etfs.parquet")
df_stocks.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/FeatureEngineeringData/stocks.parquet")
