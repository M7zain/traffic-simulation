import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

df = pd.read_csv("/data/traffic_density_202408.csv").head(1000)

round_num = 0
while True:
    round_num += 1
    print(f"--- Kafka publish round {round_num} ({len(df)} rows) ---", flush=True)
    for _, row in df.iterrows():
        data = {
            "time": str(row["DATE_TIME"]),
            "lat": float(row["LATITUDE"]),
            "lon": float(row["LONGITUDE"]),
            "min_speed": float(row["MINIMUM_SPEED"]),
            "max_speed": float(row["MAXIMUM_SPEED"]),
            "avg_speed": float(row["AVERAGE_SPEED"]),
            "vehicles": int(row["NUMBER_OF_VEHICLES"]),
            "geohash": str(row["GEOHASH"]) if pd.notna(row["GEOHASH"]) else "unknown",
        }
        producer.send("traffic-topic", value=data)
        print("sent:", data, flush=True)
        time.sleep(0.01)

    producer.flush()
    time.sleep(2)
