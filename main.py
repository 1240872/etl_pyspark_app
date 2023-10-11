from pyspark.sql import SparkSession
 from pyspark.sql.functions import col, sum, row_number, lead, datediff
 from pyspark.sql.window import Window
 from pyspark.sql import functions as F
 from pyspark.sql.types import IntegerType
 import os
 import argparse

 print("Environment Variables:", os.environ)


 def run_spark_job(source_file_path, database, table):
     # Initialize Spark session
     spark = SparkSession.builder \
         .appName("ETL Application") \
         .master("spark://spark-master:7077") \
         .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar,/opt/bitnami/spark/jars/streak.jar") \
         .getOrCreate()

     try:
         # Read the source data into a DataFrame
         df = spark.read.csv(source_file_path, sep='|', header=True, inferSchema=True)

         # Perform the transformation to find the favorite product for each customer
         aggregated_df = (df.groupBy('custId', 'productSold')
                          .agg(sum('unitsSold').alias('totalUnitsSold')))

         window_spec = Window.partitionBy('custId').orderBy(col('totalUnitsSold').desc())

         ranked_products_df = (aggregated_df.withColumn('rank', row_number().over(window_spec))
                               .filter(col('rank') == 1)
                               .select('custId', 'productSold')
                               .withColumnRenamed('productSold', 'favoriteProduct'))

         ranked_products_df.select("custId", "favoriteProduct").show()

         # calculate the longest streak for each customer
         filtered_df = df.select("custId", "transactionDate").dropDuplicates()

         window_spec_new = Window.partitionBy("custId").orderBy(col("transactionDate").desc())
         tfm_df = filtered_df.withColumn("next_transaction_date", F.lead("transactionDate", 1).over(window_spec_new))

         tfm_df.show()

         stg_df = (tfm_df
                   .withColumn("transactionDate", F.col("transactionDate").cast("date"))
                   .withColumn("next_transaction_date", F.col("next_transaction_date").cast("date"))
                   .withColumn("date_diff",
                               F.when(F.datediff(F.col("next_transaction_date"), F.col("transactionDate")) == 1,
                                      1).otherwise(0)))

         stg_df.show()

         with_list = stg_df.withColumn("date_diff_list", F.collect_list("date_diff").over(window_spec_new))
         grouped_df = (with_list
                       .groupBy("custId")
                       .agg(F.max("date_diff_list").alias("date_diff_list")))

         grouped_df.show()

         def get_longest_streak(sequence):
             longest_streak = 0
             current_streak = 0

             for value in sequence:
                 if value == 1:
                     current_streak += 1
                     longest_streak = max(longest_streak, current_streak)
                 else:
                     current_streak = 0

             return longest_streak + 1

         get_longest_streak_udf = F.udf(get_longest_streak, IntegerType())

         with_longest_streak = grouped_df.withColumn("longest_streak", get_longest_streak_udf("date_diff_list"))
         with_longest_streak.select("custId", "longest_streak").show()

         target_df = ranked_products_df.join(with_longest_streak, "custId", "inner")

         # Define the PostgreSQL connection properties
         url = f"jdbc:postgresql://postgres:5432/{database}"
         properties = {
             "user": "postgres",
             "password": "postgres",
             "driver": "org.postgresql.Driver"
         }

         # # Write the DataFrame to the Postgres SQL table
         target_df.write.jdbc(url=url, table=table, mode="overwrite", properties=properties)

         # Specify the Postgres SQL query to read data
         query = "(SELECT * FROM customers) AS tmp"

         # Read data from the Postgres SQL table into a DataFrame
         final_df = spark.read.jdbc(url=url, table=query, properties=properties)

         # Show the DataFrame
         final_df.show()

         print("ETL process completed. Results:")
     finally:
         # Stop the Spark session
         spark.stop()
 
 
 if _name_ == "_main_":
     parser = argparse.ArgumentParser(description="ETL Application")
     parser.add_argument("--source_file_path", type=str, help="Source file path")
     parser.add_argument("--database", type=str, help="Database name")
     parser.add_argument("--table", type=str, help="Table name")
     args = parser.parse_args()
 
     # Access the arguments
     source_file_path = args.source_file_path
     database = args.database
     table = args.table
     print("Arguments:", source_file_path, database, table)
     print("Arguments:", args.source_file_path, args.database, args.table)
 
     run_spark_job(args.source_file_path, args.database, args.table)