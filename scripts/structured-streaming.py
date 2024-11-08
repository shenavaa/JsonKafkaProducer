from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os
import sys

BOOTSTRAP=sys.argv[1]
TOPIC=sys.argv[2]
CHECKPOINT_LOCATION=sys.argv[3]
OUTPUT_LOCATION=sys.argv[4]


# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToParquetStream") \
    .getOrCreate()

# Define the schema for the JSON records
schema = StructType([
    StructField("samplecode", LongType(), True),
    StructField("uuid", StringType(), True),
    StructField("recordCount", LongType(), True)
])

# Kafka configurations
kafka_bootstrap_servers = BOOTSTRAP
kafka_topic_name = TOPIC
checkpoint_dir = CHECKPOINT_LOCATION
parquet_output_path = OUTPUT_LOCATION

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger",2000000) \
    .option("failOnDataLoss",False) \
    .load()


# Convert Kafka message from binary to JSON format
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

# Parse JSON and extract fields based on the schema
parsed_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Function to write batch data to Parquet
def write_to_parquet(batch_df, batch_id):
    # Write each batch as a separate Parquet file in append mode
    batch_df.withColumn('epoch',lit(batch_id)).write \
        .partitionBy("epoch") \
        .mode("append") \
        .parquet(parquet_output_path)

# Write stream to Parquet in foreachBatch mode
query = parsed_df.writeStream \
    .foreachBatch(write_to_parquet) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Await termination
query.awaitTermination()
