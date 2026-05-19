"""
TomTom Traffic Flow API -> Kafka (traffic-flow-topic).
Multiple Istanbul locations (new updates.py). ~120 samples per 10-minute cycle.
"""
import json
import os
import time
from datetime import datetime

import requests
from kafka import KafkaProducer

API_KEY = os.environ.get("TOMTOM_API_KEY", "")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "traffic-flow-topic")
SAMPLES_PER_CYCLE = int(os.environ.get("SAMPLES_PER_CYCLE", "120"))
CYCLE_SECONDS = int(os.environ.get("CYCLE_SECONDS", "600"))
SLEEP_BETWEEN = max(
    1.0,
    float(os.environ.get("SLEEP_BETWEEN_SEC", str(CYCLE_SECONDS / max(1, SAMPLES_PER_CYCLE)))),
)

# Same locations as new updates.py
LOKASYONLAR = [
    {"location_name": "Bagdat_Caddesi", "lat": 40.9706, "lon": 29.0703},
    {"location_name": "Barbaros_Bulvari", "lat": 41.0439, "lon": 29.0065},
    {"location_name": "Buyukdere_Caddesi", "lat": 41.0671, "lon": 29.0135},
    {"location_name": "E5_Kadikoy", "lat": 40.9920, "lon": 29.1006},
    {"location_name": "TEM_Maslak", "lat": 41.1115, "lon": 29.0207},
]


def fetch_flow_segment(lat, lon):
    url = (
        "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?point={lat},{lon}&key={API_KEY}"
    )
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return None, response.status_code
    seg = response.json().get("flowSegmentData") or {}
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
        print(
            f"--- API ingest cycle {cycle}: {SAMPLES_PER_CYCLE} samples "
            f"across {len(LOKASYONLAR)} locations ---",
            flush=True,
        )
        t_start = time.time()

        for i in range(SAMPLES_PER_CYCLE):
            loc = LOKASYONLAR[i % len(LOKASYONLAR)]
            seg, status = fetch_flow_segment(loc["lat"], loc["lon"])
            capture_time = datetime.now().isoformat()

            if seg is None:
                print(
                    f"API error status={status} loc={loc['location_name']}, sleeping 60s",
                    flush=True,
                )
                time.sleep(60)
                continue

            payload = {
                "capture_time": capture_time,
                "location_name": loc["location_name"],
                "lat": loc["lat"],
                "lon": loc["lon"],
                "sample_id": i + 1,
                "current_speed": seg.get("currentSpeed", 0),
                "free_flow_speed": seg.get("freeFlowSpeed", 1),
                "current_travel_time": seg.get("currentTravelTime", 0),
                "free_flow_travel_time": seg.get("freeFlowTravelTime", 0),
                "confidence": seg.get("confidence", 0.95),
            }
            producer.send(TOPIC, value=payload)
            print(f"sent {loc['location_name']} sample {i + 1}/{SAMPLES_PER_CYCLE}", flush=True)
            producer.flush()
            time.sleep(SLEEP_BETWEEN)

        elapsed = time.time() - t_start
        if elapsed < CYCLE_SECONDS:
            time.sleep(CYCLE_SECONDS - elapsed)


if __name__ == "__main__":
    main()
