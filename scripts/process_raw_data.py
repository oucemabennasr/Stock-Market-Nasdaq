import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

meta_df = spark.read.csv('/home/cloud_user/Stock-Market-Nasdaq/data/symbols_valid_meta.csv', header=True)

file_etfs = os.listdir('/home/cloud_user/Stock-Market-Nasdaq/data/etfs')

file_stocks = os.listdir('/home/cloud_user/Stock-Market-Nasdaq/data/stocks')

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

for file_name in file_etfs:
    temp_df = spark.read.csv('/home/cloud_user/Stock-Market-Nasdaq/data/etfs/' + file_name, header=True)
    temp_df = temp_df.withColumn("Symbol", lit(os.path.splitext(os.path.basename(file_name))[0]))
    df_etfs = df_etfs.union(temp_df)


for file_name in file_stocks:
    temp_df = spark.read.csv('/stock-market-dataset/stocks' + file_name, header=True)
    temp_df = temp_df.withColumn("Symbol", lit(os.path.splitext(os.path.basename(file_name))[0]))
    df_stocks = df_stocks.union(temp_df)

df_etfs = df_etfs.join(meta_df.select("Symbol", "Security Name").withColumnRenamed("Symbol", "meta_Symbol"),
                       df_etfs.Symbol == col("meta_Symbol"),
                       "inner").drop("meta_Symbol")


df_stocks = df_stocks.join(meta_df.select("Symbol", "Security Name").withColumnRenamed("Symbol", "meta_Symbol"),
                       df_stocks.Symbol == col("meta_Symbol"),
                       "inner").drop("meta_Symbol")

df_etfs = df_etfs.withColumn("Symbol", col("Symbol").cast("string"))
df_etfs = df_etfs.withColumn("Security Name", col("Security Name").cast("string"))
df_etfs = df_etfs.withColumn("Date", col("Date").cast("string"))
df_etfs = df_etfs.withColumn("Open", col("Open").cast("float"))
df_etfs = df_etfs.withColumn("High", col("High").cast("float"))
df_etfs = df_etfs.withColumn("Low", col("Low").cast("float"))
df_etfs = df_etfs.withColumn("Close", col("Close").cast("float"))
df_etfs = df_etfs.withColumn("Adj Close", col("Adj Close").cast("float"))
df_etfs = df_etfs.withColumn("Volume", col("Volume").cast("int"))

df_stocks = df_stocks.withColumn("Symbol", col("Symbol").cast("string"))
df_stocks = df_stocks.withColumn("Security Name", col("Security Name").cast("string"))
df_stocks = df_stocks.withColumn("Date", col("Date").cast("string"))
df_stocks = df_stocks.withColumn("Open", col("Open").cast("float"))
df_stocks = df_stocks.withColumn("High", col("High").cast("float"))
df_stocks = df_stocks.withColumn("Low", col("Low").cast("float"))
df_stocks = df_stocks.withColumn("Close", col("Close").cast("float"))
df_stocks = df_stocks.withColumn("Adj Close", col("Adj Close").cast("float"))
df_stocks = df_stocks.withColumn("Volume", col("Volume").cast("int"))

df_etfs.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/data/parquet_file/etfs.parquet")

df_stocks.write.parquet("/home/cloud_user/Stock-Market-Nasdaq/data/parquet_file/stocks.parquet")
