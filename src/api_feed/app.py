"""
TomTom Traffic Flow API -> Kafka (traffic-flow-topic).
120 samples per 10-minute cycle (~5s between calls) to stay within practical API pacing.
"""
import json
import os
import time
from datetime import datetime

import requests
from kafka import KafkaProducer

API_KEY = os.environ.get("TOMTOM_API_KEY", "")
LAT = float(os.environ.get("TOMTOM_LAT", "41.0450"))
LON = float(os.environ.get("TOMTOM_LON", "29.0350"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "traffic-flow-topic")
SAMPLES_PER_CYCLE = int(os.environ.get("SAMPLES_PER_CYCLE", "120"))
CYCLE_SECONDS = int(os.environ.get("CYCLE_SECONDS", "600"))
# Space calls evenly across the cycle (fallback 5s if env math is off)
SLEEP_BETWEEN = max(1.0, float(os.environ.get("SLEEP_BETWEEN_SEC", str(CYCLE_SECONDS / max(1, SAMPLES_PER_CYCLE)))))


def fetch_flow_segment():
    url = (
        "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?point={LAT},{LON}&key={API_KEY}"
    )
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        return None, response.status_code
    data = response.json()
    seg = data.get("flowSegmentData") or {}
    return seg, 200


def main():
    while not API_KEY:
        print("ERROR: TOMTOM_API_KEY is not set. Create .env from .env.example and restart.", flush=True)
        time.sleep(60)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    cycle = 0
    while True:
        cycle += 1
        print(f"--- API ingest cycle {cycle}: up to {SAMPLES_PER_CYCLE} samples ---", flush=True)
        t_start = time.time()

        for i in range(SAMPLES_PER_CYCLE):
            seg, status = fetch_flow_segment()
            now = datetime.utcnow()
            capture_time = now.isoformat() + "Z"

            if seg is None:
                print(f"API error status={status}, sleeping 60s", flush=True)
                time.sleep(60)
                continue

            payload = {
                "capture_time": capture_time,
                "lat": LAT,
                "lon": LON,
                "sample_id": i + 1,
                "current_speed": seg.get("currentSpeed"),
                "free_flow_speed": seg.get("freeFlowSpeed"),
                "current_travel_time": seg.get("currentTravelTime"),
                "free_flow_travel_time": seg.get("freeFlowTravelTime"),
                "confidence": seg.get("confidence"),
            }
            producer.send(TOPIC, value=payload)
            print(f"sent sample {i + 1}/{SAMPLES_PER_CYCLE}: {payload}", flush=True)
            producer.flush()

            time.sleep(SLEEP_BETWEEN)

        elapsed = time.time() - t_start
        if elapsed < CYCLE_SECONDS:
            time.sleep(CYCLE_SECONDS - elapsed)


if __name__ == "__main__":
    main()
