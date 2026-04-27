"""
Structured Streaming: Kafka -> parse JSON -> foreachBatch -> MongoDB.
Submit with spark-submit (--master, --packages); jars come from CLI, not SparkSession.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CHECKPOINT_DIR = "/checkpoints/traffic-kafka"
MONGO_URI = "mongodb://mongo:27017/traffic_db.traffic"

spark = SparkSession.builder.appName("KafkaToMongo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("time", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("avg_speed", DoubleType()),
        StructField("vehicles", IntegerType()),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "traffic-topic")
    .option("startingOffsets", "earliest")
    # Local dev: checkpoint + Kafka topic resets (compose down/up, retention) cause offset mismatch; don't kill the query.
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)


def write_to_mongo(batch_df, batch_id):
    (
        batch_df.write.format("mongodb")
        .mode("append")
        .option("spark.mongodb.write.connection.uri", MONGO_URI)
        .save()
    )


query = (
    parsed.writeStream.foreachBatch(write_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

query.awaitTermination()
