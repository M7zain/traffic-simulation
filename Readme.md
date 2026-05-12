# Trafik akış (streaming) hattı

**Docker Compose** ile çalışan servisler: **TomTom API** verisini Kafkaya atan **api-feed**, **Kafka**, **Spark** (ön işleme + **joblib** model), **MongoDB** (ham özellikler + tahminler), **Superset** ve isteğe bağlı **CSV producer** (ayrı profil). **Spark Structured Streaming** `traffic-flow-topic` topiğini okur; veriyi modelle uyumlu şekilde işler, tahmin üretir ve **MongoDB**ye iki koleksiyonda yazar.

**Gereksinim:** [Docker Desktop](https://docs.docker.com/desktop/) (Windows / WSL2).

**API anahtarı:** Depo kökünde `.env` dosyası oluşturun (örnek: `.env.example`). `TOMTOM_API_KEY` burada olmalı; Compose bu dosyayı değişkenler için okur.

---

## Hızlı başlangıç

| Amaç | Komut |
|------|--------|
| İmajları derleyip tüm servisleri başlat | `docker compose up -d --build` |
| Her şeyi durdur | `docker compose down` |
| Çalışan servisleri listele | `docker compose ps` |

**Sadece CSV ile eski producer** (Kafka `traffic-topic` vb.): `docker compose --profile csv up -d --build`

---

## Log ve izleme

**api-feed, Kafka ve Spark streaming** çıktısını aynı anda izlemek:

```bash
docker compose logs -f api-feed kafka spark-streaming
```

**Sadece Spark streaming:**

```bash
docker compose logs -f spark-streaming
```

**Spark Standalone arayüzü:** [http://localhost:8080](http://localhost:8080)

---

## Hattı doğrulama

**Kafkadan mesaj oku** (api-feedin gönderdiği `traffic-flow-topic`):

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic traffic-flow-topic --from-beginning --max-messages 5
```

Durdurmak için: `Ctrl+C`

**MongoDBde kayıt sayıları** (`traffic_db` veritabanı):

```bash
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('traffic_db').tomtom_preprocessed.countDocuments({})"
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('traffic_db').tomtom_predictions.countDocuments({})"
```

Spark loglarında `[batch …] predictions` satırları görünüyorsa model tarafı da çalışıyordur.

---

## Klasör yapısı (özet)

| Yol | İş |
|-----|-----|
| `src/api_feed/` | TomTom flow API → Kafka `traffic-flow-topic` |
| `src/spark_jobs/spark_app.py` | Kafka → ön işleme + model → MongoDB |
| `src/model/traffic_model.joblib` | Spark konteynerinde `/models` olarak bağlanır |
| `src/producer/` | İsteğe bağlı CSV → Kafka (Compose profili `csv`) |
| `data/` | CSV producer için (isteğe bağlı) |
| `docker-compose.yml` | Servis tanımları |

---

## Windows: izleme betiği

Depo kökünde:

```powershell
.\watch-flow.ps1
```

Stacki açar ve `api-feed`, `kafka`, `spark-streaming` loglarını takip eder.
