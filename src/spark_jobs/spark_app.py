"""
Kafka (TomTom-style flow messages) -> Spark preprocessing (aligned with needs-integration.py)
-> joblib model -> MongoDB (preprocessed + predictions).
"""
import os

import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofweek,
    from_json,
    hour,
    lit,
    minute,
    to_timestamp,
    when,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Model expects this exact column order (needs-integration.py / training).
MODEL_FEATURES = [
    "ID",
    "Day",
    "CurrentSpeed",
    "FreeFlowSpeed",
    "CurrentTravelTime",
    "FreeFlowTravelTime",
    "Confidence",
    "Hour",
    "Minute",
    "DayOfWeek",
    "IsWeekend",
]

CHECKPOINT_DIR = "/checkpoints/traffic-flow"
MONGO_PREPROCESSED_URI = os.environ.get(
    "MONGO_PREPROCESSED_URI",
    "mongodb://mongo:27017/traffic_db.tomtom_preprocessed",
)
MONGO_PREDICTIONS_URI = os.environ.get(
    "MONGO_PREDICTIONS_URI",
    "mongodb://mongo:27017/traffic_db.tomtom_predictions",
)
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/traffic_model.joblib")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic-flow-topic")

_MODEL = None


def get_model():
    global _MODEL
    if _MODEL is None:
        _MODEL = joblib.load(MODEL_PATH)
    return _MODEL


spark = SparkSession.builder.appName("TrafficFlowML").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_schema = StructType(
    [
        StructField("capture_time", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("sample_id", LongType()),
        StructField("current_speed", DoubleType()),
        StructField("free_flow_speed", DoubleType()),
        StructField("current_travel_time", DoubleType()),
        StructField("free_flow_travel_time", DoubleType()),
        StructField("confidence", DoubleType()),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), kafka_schema).alias("data"))
    .select("data.*")
)


def write_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    base = batch_df.withColumn("event_time", to_timestamp(col("capture_time"))).dropna(
        subset=[
            "event_time",
            "lat",
            "lon",
            "sample_id",
            "current_speed",
            "free_flow_speed",
            "current_travel_time",
            "free_flow_travel_time",
            "confidence",
        ]
    )

    # Spark dayofweek: 1=Sunday .. 7=Saturday. Python weekday(): Monday=0 .. Sunday=6.
    dow_py = ((dayofweek(col("event_time")) + lit(5)) % lit(7)).cast("int")

    enriched = (
        base.withColumn("Hour", hour(col("event_time")))
        .withColumn("Minute", minute(col("event_time")))
        .withColumn("DayOfWeek", dow_py)
        .withColumn("Day", col("DayOfWeek"))
        .withColumn("IsWeekend", when(col("DayOfWeek") >= lit(5), lit(1)).otherwise(lit(0)))
        .withColumnRenamed("sample_id", "ID")
        .withColumnRenamed("current_speed", "CurrentSpeed")
        .withColumnRenamed("free_flow_speed", "FreeFlowSpeed")
        .withColumnRenamed("current_travel_time", "CurrentTravelTime")
        .withColumnRenamed("free_flow_travel_time", "FreeFlowTravelTime")
        .withColumnRenamed("confidence", "Confidence")
    )

    feature_df = enriched.select(
        "capture_time",
        "lat",
        "lon",
        *MODEL_FEATURES,
    )

    pdf = feature_df.toPandas()
    if pdf.empty:
        return

    for c in MODEL_FEATURES:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

    pdf = pdf.dropna(subset=MODEL_FEATURES)
    if pdf.empty:
        return

    model = get_model()
    X = pdf[MODEL_FEATURES]
    pdf["prediction"] = model.predict(X)
    pdf["batch_id"] = int(batch_id)

    print(f"[batch {batch_id}] predictions (sample):", flush=True)
    print(pdf[["ID", "prediction"]].head(15).to_string(index=False), flush=True)

    pre_pdf = pdf.drop(columns=["prediction"], errors="ignore")
    pred_pdf = pdf[["batch_id", "ID", "prediction", "capture_time", "lat", "lon"]].rename(
        columns={"ID": "sample_id"}
    )

    pre_spark = spark.createDataFrame(pre_pdf)
    pred_spark = spark.createDataFrame(pred_pdf)

    (
        pre_spark.write.format("mongodb")
        .mode("append")
        .option("spark.mongodb.write.connection.uri", MONGO_PREPROCESSED_URI)
        .save()
    )
    (
        pred_spark.write.format("mongodb")
        .mode("append")
        .option("spark.mongodb.write.connection.uri", MONGO_PREDICTIONS_URI)
        .save()
    )


query = (
    parsed.writeStream.foreachBatch(write_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

query.awaitTermination()
