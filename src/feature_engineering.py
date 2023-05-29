import os
import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, when, row_number, expr, percentile_approx
#from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType
from pyspark.sql.window import Window



#@pandas_udf("float", PandasUDFType.GROUPED_AGG)
#def calculate_median(arr):
#    arr_copy = arr.copy()
#    return arr_copy.median()

def calculate_moving_avg(df):
    window_spec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("vol_moving_avg",
                       when(row_number().over(window_spec) > 29,
                            avg("Volume").over(window_spec)).otherwise(None))
    return df

def calculate_rolling_median(df):
    window_spec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("adj_close_rolling_med",
                       when(row_number().over(window_spec) > 29,
                            expr("percentile_approx(`Adj_Close`, 0.5)").over(window_spec)).otherwise(None))
    return df

def write_parquet_file(df, directory_output, data):
    num_partitions = 10
    df = df.repartition(num_partitions)
    df.write.parquet(os.path.join(directory_output, f'{data}.parquet'))

def process_data(directory_path, data, directory_output):
    spark = SparkSession.builder.appName("FeatureEngineering") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.shuffle.file.buffer", "1m") \
        .config("spark.shuffle.unsafe.file.output.buffer", "2m") \
        .getOrCreate()

    df = spark.read.parquet(os.path.join(directory_path, f'{data}.parquet'))
    df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))
    df = calculate_moving_avg(df)
    df = calculate_rolling_median(df)
    df = df.withColumn("Date", col("Date").cast(StringType()))
    df.cache()

    write_parquet_file(df, directory_output, data)

def main():
    parser = argparse.ArgumentParser(description='Process stock market data files.')
    parser.add_argument('directory', type=str, help='Directory path containing the data parquet files')
    parser.add_argument('data', type=str, choices=['etfs', 'stocks'], help='Data to process (etfs or stocks)')
    parser.add_argument('directory_output', type=str, help='Directory path containing the output data')

    args = parser.parse_args()

    directory_path = args.directory
    data = args.data
    directory_output = args.directory_output

    process_data(directory_path, data, directory_output)

if __name__ == '__main__':
    main()

