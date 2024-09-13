from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WebServerLogAnalysis").getOrCreate()

# Read log files
logs_df = spark.read.text("path/to/your/logs")

# Extract relevant fields (adjust based on your log format)
logs_df = logs_df.select(
    F.split(F.col("value"), " ").getItem(0).alias("timestamp"),
    F.split(F.col("value"), " ").getItem(6).alias("url"),
    F.split(F.col("value"), " ").getItem(9).alias("referrer")
)

# Map and reduce to count occurrences of URLs
url_counts = logs_df.groupBy("url").count().orderBy(F.desc("count"))

# Show results
url_counts.show()
