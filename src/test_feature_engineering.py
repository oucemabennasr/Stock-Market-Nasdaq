import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, DoubleType
from feature_engineering import calculate_moving_avg, calculate_rolling_median

class FeatureEngineeringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for unit tests
        cls.spark = SparkSession.builder.appName("UnitTest").getOrCreate()

        # Create a test DataFrame
        schema = StructType([
            StructField("Symbol", StringType(), nullable=False),
            StructField("Date", StringType(), nullable=False),
            StructField("Volume", IntegerType(), nullable=True),
            StructField("Adj_Close", DoubleType(), nullable=True)
        ])
        data = [
            ("AAPL", "2023-05-01", 1000, 150.0),
            ("AAPL", "2023-05-02", 2000, 160.0),
            ("AAPL", "2023-05-03", 1500, 170.0),
            ("AAPL", "2023-05-04", 2500, 180.0),
            ("AAPL", "2023-05-05", 1800, 190.0),
            ("AAPL", "2023-05-06", 1200, 200.0),
            ("AAPL", "2023-05-07", 3000, 210.0),
            ("GOOG", "2023-05-01", 500, 1200.0),
            ("GOOG", "2023-05-02", 800, 1250.0),
            ("GOOG", "2023-05-03", 700, 1300.0),
            ("GOOG", "2023-05-04", 1200, 1350.0),
            ("GOOG", "2023-05-05", 900, 1400.0),
            ("GOOG", "2023-05-06", 600, 1450.0),
            ("GOOG", "2023-05-07", 1500, 1500.0)
        ]
        cls.test_df = cls.spark.createDataFrame(data, schema)

    def test_calculate_moving_avg(self):
        expected_df = self.test_df.withColumn("vol_moving_avg",
						expr("CASE WHEN row_number() OVER (PARTITION BY Symbol ORDER BY Date) > 29 THEN AVG(Volume) OVER (PARTITION BY Symbol ORDER BY Date) END"))
        result_df = calculate_moving_avg(self.test_df)
        self.assertTrue(result_df.columns == expected_df.columns)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_rolling_median(self):
        expected_df = self.test_df.withColumn("adj_close_rolling_med",
						expr("CASE WHEN row_number() OVER (PARTITION BY Symbol ORDER BY Date) > 39 THEN percentile_approx(Adj_Close, 0.5) OVER (PARTITION BY Symbol ORDER BY Date) END"))
        result_df = calculate_rolling_median(self.test_df)
        self.assertTrue(result_df.columns == expected_df.columns)
        self.assertEqual(result_df.collect(), expected_df.collect())

    @classmethod
    def tearDownClass(cls):
        # Stop SparkSession after unit tests
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()

