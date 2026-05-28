import os

# Environment Variables
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\Program Files\Java\jdk-17\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Advanced ETL Pipeline") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

print("Spark Started Successfully ")

# Read CSV File
df = spark.read.csv(
    r"C:\Users\MUMMANA TRINADH\OneDrive\Desktop\ETL-Pipeline\data\train.csv",
    header=True,
    inferSchema=True
)

# Convert Sales Column to Float
df = df.withColumn("Sales", col("Sales").cast("float"))

# Print Schema
print("Schema:")
df.printSchema()

# Show Dataset
print("Dataset Preview")
df.show(15)

# Total Rows
print("Total Rows:")
print(df.count())

# Remove Null Values
clean_df = df.dropna()

print("After Removing Null Values")
clean_df.show(15)

# Remove Duplicates
clean_df = clean_df.dropDuplicates()

print("After Removing Duplicates")
print(clean_df.count())

# Rename Column
rename_df = clean_df.withColumnRenamed(
    "Customer Name",
    "Customer_Name"
)

print("Columns Renamed")
rename_df.show(15)

# Sorting
sorted_df = rename_df.orderBy(col("Sales").desc())

print("Top Sales")
sorted_df.show(15)

# Group By + Aggregation
group_df = rename_df.groupBy("Category") \
    .agg(avg("Sales").alias("Average_Sales"))

print("Average Sales By Category")
group_df.show()

# Filter Data
filtered_df = rename_df.filter(col("Sales") > 500)

print("Sales Greater Than 500")
filtered_df.show(15)

# Spark SQL
rename_df.createOrReplaceTempView("sales_data")

print("Spark SQL Result")

spark.sql("""
SELECT Category, COUNT(*) AS Total_Orders
FROM sales_data
GROUP BY Category
""").show()

# Save Final Output CSV
rename_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        r"C:\Users\MUMMANA TRINADH\OneDrive\Desktop\ETL-Pipeline\output\final_data"
    )

print("Final Data Saved Successfully ")

print("ETL Pipeline Completed Successfully ")

# Stop Spark
spark.stop()