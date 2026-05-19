"""
Kafka -> Spark preprocessing (new updates.py) -> classifier -> MongoDB.
"""
import os
import traceback

import joblib
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
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

CHECKPOINT_DIR = "/checkpoints/traffic-flow-v2"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "traffic_db")
MONGO_PREPROCESSED_COLLECTION = os.environ.get(
    "MONGO_PREPROCESSED_COLLECTION", "tomtom_preprocessed"
)
MONGO_PREDICTIONS_COLLECTION = os.environ.get(
    "MONGO_PREDICTIONS_COLLECTION", "tomtom_predictions"
)
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/traffic_classifier_new.joblib")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic-flow-topic")

_MODEL = None
_FEATURES = None


def get_model_bundle():
    global _MODEL, _FEATURES
    if _MODEL is None:
        bundle = joblib.load(MODEL_PATH)
        if isinstance(bundle, dict) and "model" in bundle and "features" in bundle:
            _MODEL = bundle["model"]
            _FEATURES = list(bundle["features"])
        else:
            raise ValueError(
                f"Expected joblib dict with 'model' and 'features' at {MODEL_PATH}"
            )
        print(f"Model loaded from {MODEL_PATH}, features={_FEATURES}", flush=True)
    return _MODEL, _FEATURES


def write_mongo_pandas(pdf, collection):
    if pdf.empty:
        return
    records = pdf.to_dict(orient="records")
    client = MongoClient(MONGO_URI)
    client[MONGO_DB][collection].insert_many(records)
    client.close()


spark = SparkSession.builder.appName("TrafficFlowML").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_schema = StructType(
    [
        StructField("capture_time", StringType()),
        StructField("location_name", StringType()),
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
    if batch_df.limit(1).count() == 0:
        return

    try:
        base = (
            batch_df.withColumn("event_time", to_timestamp(col("capture_time")))
            .withColumn("current_speed", coalesce(col("current_speed"), lit(0.0)))
            .withColumn("free_flow_speed", coalesce(col("free_flow_speed"), lit(1.0)))
            .withColumn("current_travel_time", coalesce(col("current_travel_time"), lit(0.0)))
            .withColumn(
                "free_flow_travel_time", coalesce(col("free_flow_travel_time"), lit(0.0))
            )
            .withColumn("confidence", coalesce(col("confidence"), lit(0.95)))
            .dropna(subset=["event_time", "lat", "lon", "location_name"])
        )

        if base.limit(1).count() == 0:
            return

        # Python weekday(): Monday=0 .. Sunday=6 (matches new updates.py Day + DayOfWeek)
        dow_py = ((dayofweek(col("event_time")) + lit(5)) % lit(7)).cast("int")

        enriched = (
            base.withColumn("Hour", hour(col("event_time")))
            .withColumn("Minute", minute(col("event_time")))
            .withColumn("DayOfWeek", dow_py)
            .withColumn("Day", col("DayOfWeek"))
            .withColumn("IsWeekend", when(col("DayOfWeek") >= lit(5), lit(1)).otherwise(lit(0)))
            .withColumnRenamed("current_speed", "CurrentSpeed")
            .withColumnRenamed("free_flow_speed", "FreeFlowSpeed")
            .withColumnRenamed("current_travel_time", "CurrentTravelTime")
            .withColumnRenamed("free_flow_travel_time", "FreeFlowTravelTime")
            .withColumnRenamed("confidence", "Confidence")
        )

        model, features = get_model_bundle()

        meta_cols = ["capture_time", "location_name", "lat", "lon", "sample_id"]
        feature_df = enriched.select(*meta_cols, *features)

        pdf = feature_df.toPandas()
        if pdf.empty:
            return

        for c in features:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

        pdf = pdf.dropna(subset=features)
        if pdf.empty:
            return

        X = pdf[features]
        pdf["prediction"] = model.predict(X)
        pdf["batch_id"] = int(batch_id)

        print(f"[batch {batch_id}] wrote {len(pdf)} rows", flush=True)
        print(
            pdf[["location_name", "prediction"]].head(10).to_string(index=False),
            flush=True,
        )

        pred_pdf = pdf[
            ["batch_id", "sample_id", "location_name", "prediction", "capture_time", "lat", "lon"]
            + features
        ]

        write_mongo_pandas(pdf.drop(columns=["prediction"]), MONGO_PREPROCESSED_COLLECTION)
        write_mongo_pandas(pred_pdf, MONGO_PREDICTIONS_COLLECTION)
        print(f"[batch {batch_id}] Mongo OK -> {MONGO_DB}", flush=True)

    except Exception:
        print(f"[batch {batch_id}] ERROR:", flush=True)
        traceback.print_exc()
        raise


query = (
    parsed.writeStream.foreachBatch(write_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

query.awaitTermination()
