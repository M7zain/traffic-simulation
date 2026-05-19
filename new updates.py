import joblib
import requests
import pandas as pd
import datetime
import time

data = joblib.load(r"D:\BigData\APItest\traffic_classifier.joblib")

model = data["model"]
FEATURES = data["features"]

print("Model yüklendi.")
API_KEY = "dyc6ltd5M95sFJPoxVbGRJxDeCkU0bLO"

LOKASYONLAR = [
    {"isim": "Bagdat_Caddesi", "lat": 40.9706, "lon": 29.0703},
    {"isim": "Barbaros_Bulvari", "lat": 41.0439, "lon": 29.0065},
    {"isim": "Buyukdere_Caddesi", "lat": 41.0671, "lon": 29.0135},
    {"isim": "E5_Kadikoy", "lat": 40.9920, "lon": 29.1006},
    {"isim": "TEM_Maslak", "lat": 41.1115, "lon": 29.0207}
]

TOPLAM_KAYIT = 2
BEKLEME_SURESI = 5

day_mapping = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6
}

def veri_topla():

    kayit_sayisi = 0

    print("\nVeri toplama başladı.")
    print(f"Hedef kayıt: {TOPLAM_KAYIT}")

    while kayit_sayisi < TOPLAM_KAYIT:

        for lokasyon in LOKASYONLAR:

            lat = lokasyon["lat"]
            lon = lokasyon["lon"]
            isim = lokasyon["isim"]

            url = (
                f"https://api.tomtom.com/traffic/services/4/flowSegmentData/"
                f"absolute/10/json?point={lat},{lon}&key={API_KEY}"
            )

            try:
                response = requests.get(url, timeout=10)

                if response.status_code != 200:
                    print(f"API Hatası: {response.status_code}")
                    continue

                data_api = response.json()["flowSegmentData"]

                simdi = datetime.datetime.now()

                
                current_speed = data_api.get("currentSpeed", 0)
                free_flow_speed = data_api.get("freeFlowSpeed", 1)
                current_tt = data_api.get("currentTravelTime", 0)
                free_flow_tt = data_api.get("freeFlowTravelTime", 0)
                confidence = data_api.get("confidence", 0.95)

                hour = simdi.hour
                minute = simdi.minute
                day_of_week = simdi.weekday()
                is_weekend = 1 if day_of_week in [5, 6] else 0

                day_encoded = day_mapping[simdi.strftime("%A")]

                X_new = pd.DataFrame([{
                    "Day": day_encoded,
                    "CurrentSpeed": current_speed,
                    "FreeFlowSpeed": free_flow_speed,
                    "CurrentTravelTime": current_tt,
                    "FreeFlowTravelTime": free_flow_tt,
                    "Confidence": confidence,
                    "Hour": hour,
                    "Minute": minute,
                    "DayOfWeek": day_of_week,
                    "IsWeekend": is_weekend
                }])

                X_new = X_new[FEATURES]

                prediction = model.predict(X_new)[0]

                print("\n==============================")
                print(f"Lokasyon: {isim}")
                print(f"Koordinat: {lat}, {lon}")
                print(f"Saat: {simdi}")
                print(f"Anlık hız: {current_speed}")
                print(f"Trafik durumu: {prediction}")
                print("==============================")

            except Exception as e:
                print("\nHATA:", e)

        kayit_sayisi += 1

        if kayit_sayisi < TOPLAM_KAYIT:
            time.sleep(BEKLEME_SURESI)

    print("\nİşlem tamamlandı.")

veri_topla()