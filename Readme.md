# Trafik akış (streaming) hattı

**Docker Compose** ile çalışan servisler: **TomTom API** → **Kafka** → **Spark** (model) → **MongoDB** + **PostgreSQL** (Superset grafikleri), **Superset** arayüzü ve isteğe bağlı **CSV producer** (ayrı profil). **Spark Structured Streaming** `traffic-flow-topic` topiğini okur; veriyi modelle uyumlu şekilde işler, tahmin üretir ve **MongoDB**ye iki koleksiyonda yazar.

**Gereksinim:** [Docker Desktop](https://docs.docker.com/desktop/) (Windows / WSL2).

**API anahtarı:** Depo kökünde `.env` dosyası oluşturun (örnek: `.env.example`). Yalnızca `TOMTOM_API_KEY` gerekir. Beş İstanbul lokasyonu `src/api_feed/app.py` içinde tanımlıdır (`.env` ile verilmez).

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

**Son kayıtları görüntüle** (tahminler — lokasyon, trafik sınıfı, hız, zaman):

```bash
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('traffic_db').tomtom_predictions.find({},{location_name:1,prediction:1,CurrentSpeed:1,Hour:1,Minute:1,capture_time:1,_id:0}).sort({capture_time:-1}).limit(10).forEach(d=>printjson(d))"
```

**Ön işlenmiş örnek** (model girdisi alanları):

```bash
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('traffic_db').tomtom_preprocessed.find({},{location_name:1,Day:1,CurrentSpeed:1,Confidence:1,IsWeekend:1,capture_time:1,_id:0}).sort({capture_time:-1}).limit(5).forEach(d=>printjson(d))"
```

Spark loglarında `Model loaded` ve `Mongo + Postgres OK` görünüyorsa akış çalışıyordur.

---

## Superset (grafik arayüz)

Spark tahminleri **MongoDB**ye ve Superset için **PostgreSQL**e (`traffic_viz`) yazar. Superset MongoDBye doğrudan bağlanmaz.

Stack çalışırken tarayıcıda **http://localhost:8088** adresine gidin; giriş ekranında aşağıdaki bilgileri kullanın.

| | |
|--|--|
| URL | [http://localhost:8088](http://localhost:8088) |
| Kullanıcı adı | `admin` |
| Şifre | `admin` |
| Veritabanı bağlantısı | `Traffic Postgres` (otomatik eklenir) |
| Datasetler | `tomtom_predictions`, `tomtom_preprocessed` |

**İlk grafik (örnek):**

1. **Charts** → **+ Chart** → dataset: `tomtom_predictions`
2. **Bar chart** → X: `location_name`, Metric: **Count**
3. Kaydet → **Dashboards** → **+ Dashboard** → grafiği ekle

**Tahmin dağılımı:** Chart tipi **Pie**, Dimension: `prediction`

Superset ilk açılışta 1–2 dakika sürebilir (`docker compose logs -f superset`).

---

## Klasör yapısı (özet)

| Yol | İş |
|-----|-----|
| `src/api_feed/` | TomTom flow API → Kafka `traffic-flow-topic` |
| `src/spark_jobs/spark_app.py` | Kafka → ön işleme + model → MongoDB |
| `src/model/traffic_classifier_new.joblib` | Spark konteynerinde `/models` olarak bağlanır (Git’e eklenmez) |
| `src/producer/` | İsteğe bağlı CSV → Kafka (Compose profili `csv`) |
| `data/` | CSV producer için (isteğe bağlı) |
| `src/superset/` | Superset imajı, Postgres tabloları, otomatik dataset |
| `docker-compose.yml` | Servis tanımları |

---

## Windows: izleme betiği

Depo kökünde:

```powershell
.\watch-flow.ps1
```

Stacki açar ve `api-feed`, `kafka`, `spark-streaming` loglarını takip eder.
