import unittest
from main import run_spark_job
from pyspark.sql import SparkSession


class TestIntegration(unittest.TestCase):

    def setUp(self):
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("ETL Test Application") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
            .getOrCreate()

    def tearDown(self):
        # Stop the Spark session
        self.spark.stop()

    def test_run_spark_job(self):
        source_path = "/opt/data/transaction.csv"
        database = "postgres"
        table = "customers"

        # run the spark job
        test_df = run_spark_job(source_path, database, table, self.spark)
        test_longest_streak = test_df.select(col("longest_streak")).filter((col("customer_id") == "0023938"))

        self.assertEqual(len(test_df.columns), 3)
        self.assertEqual(test_df.columns[0], "customer_id")
        self.assertEqual(test_df.columns[1], "favourite_product")
        self.assertEqual(test_df.columns[2], "longest_streak")
        self.assertEqual(test_df.dtypes[0][1], "int")
        self.assertEqual(test_df.count(), 885)
        self.assertEqual(test_longest_streak.collect()[0][0], 2)


if __name__ == '__main__':
    unittest.main()
