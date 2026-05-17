from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, when, desc
from pyspark.sql.types import StructType, StringType, BooleanType

spark = SparkSession.builder \
    .appName("WikimediaKafkaSparkStreamingBonus") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Streaming event schema
schema = StructType() \
    .add("wiki", StringType()) \
    .add("title", StringType()) \
    .add("user", StringType()) \
    .add("type", StringType()) \
    .add("bot", BooleanType()) \
    .add("timestamp", StringType())

# Static dataset for bonus Spark SQL enrichment
mapping_df = spark.read.csv(
    "data/wiki_mapping.csv",
    header=True,
    inferSchema=True
)

# Read Kafka stream
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "wiki-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka
events_df = raw_df.selectExpr("CAST(value AS STRING) AS json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("wiki").isNotNull())

# Human/Bot classification
clean_events = events_df.withColumn(
    "actor_type",
    when(col("bot") == True, "Bot").otherwise("Human")
)

# BONUS: join streaming data with static dataset
enriched_events = clean_events.join(
    mapping_df,
    on="wiki",
    how="left"
).fillna({
    "language": "Unknown",
    "category": "Unknown"
})

# Process each micro-batch
def process_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    # Events by wiki
    wiki_counts = batch_df.groupBy(
        "wiki",
        "language",
        "category",
        "actor_type"
    ).agg(
        count("*").alias("event_count")
    ).orderBy(desc("event_count"))

    # Events by type
    type_counts = batch_df.groupBy(
        "type"
    ).agg(
        count("*").alias("event_count")
    ).orderBy(desc("event_count"))

    # Top users
    top_users = batch_df.groupBy(
        "user",
        "actor_type"
    ).agg(
        count("*").alias("event_count")
    ).orderBy(desc("event_count")).limit(20)

    # Activity analysis
    wiki_activity = batch_df.groupBy(
        "wiki",
        "language",
        "category"
    ).agg(
        count("*").alias("event_count")
    ).withColumn(
        "activity_status",
        when(col("event_count") >= 20, "High Activity")
        .otherwise("Normal")
    ).orderBy(desc("event_count"))

    # Language analytics (bonus)
    language_counts = batch_df.groupBy(
        "language"
    ).agg(
        count("*").alias("event_count")
    ).orderBy(desc("event_count"))

    # Category analytics (bonus)
    category_counts = batch_df.groupBy(
        "category"
    ).agg(
        count("*").alias("event_count")
    ).orderBy(desc("event_count"))

    # Persistent Hive-style Parquet storage
    wiki_counts.write.mode("append").parquet("warehouse/wiki_counts")
    type_counts.write.mode("append").parquet("warehouse/type_counts")
    top_users.write.mode("append").parquet("warehouse/top_users")
    wiki_activity.write.mode("append").parquet("warehouse/wiki_activity")
    language_counts.write.mode("append").parquet("warehouse/language_counts")
    category_counts.write.mode("append").parquet("warehouse/category_counts")

    # Dashboard latest data
    wiki_counts.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("dashboard_data/wiki_counts")

    type_counts.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("dashboard_data/type_counts")

    top_users.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("dashboard_data/top_users")

    wiki_activity.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("dashboard_data/wiki_activity")

    language_counts.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("dashboard_data/language_counts")

    category_counts.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("dashboard_data/category_counts")

    print(f"\n===== Batch {batch_id} processed =====")

    wiki_counts.show(truncate=False)
    language_counts.show(truncate=False)
    category_counts.show(truncate=False)

# Start streaming
query = enriched_events.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "checkpoints/wikimedia_main") \
    .trigger(processingTime="1 second") \
    .start()

query.awaitTermination()