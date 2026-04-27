# Trafik akış (streaming) hattı

**Docker Compose** ile çalışan servisler: **Kafka**, **Spark**, **MongoDB**, **Superset** ve CSV satırlarını Kafkaya gönderen **Python producer**; **Spark Structured Streaming** bu topiği okuyup toplu verileri **MongoDB**ye yazar.

**Gereksinim:** [Docker Desktop](https://docs.docker.com/desktop/) (Windows / WSL2).

---

## Hızlı başlangıç

| Amaç | Komut |
|------|--------|
| İmajları derleyip tüm servisleri başlat | `docker compose up -d --build` |
| Her şeyi durdur | `docker compose down` |
| Çalışan servisleri listele | `docker compose ps` |

---

## Log ve izleme

**Producer, Kafka ve Spark streaming** çıktısını aynı anda izlemek:

```bash
docker compose logs -f producer kafka spark-streaming
```

**Sadece Spark streaming:**

```bash
docker compose logs -f spark-streaming
```

**Spark Standalone arayüzü:** [http://localhost:8080](http://localhost:8080)

---

## Hattı doğrulama

**Kafkadan mesaj oku** (producerın gönderdiği `traffic-topic`):

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic traffic-topic --from-beginning
```

Durdurmak için: `Ctrl+C`

**MongoDBde Sparkın yazdığı kayıt sayısı** (`traffic_db` veritabanı, `traffic` koleksiyonu):

```bash
docker compose exec mongo mongosh --quiet --eval "db.traffic.countDocuments()" traffic_db
```

---

## Klasör yapısı (özet)

| Yol | İş |
|-----|-----|
| `src/producer/` | JSON satırlarını `traffic-topic`e yollar |
| `src/spark_jobs/spark_app.py` | Kafka → MongoDB akış işi |
| `data/` | Producer için `traffic_density_202408.csv` buraya (Gitte isteğe bağlı) |
| `docker-compose.yml` | Servis tanımları |

---

## Windows: izleme betiği

Depo kökünde:

```powershell
.\watch-flow.ps1
```

Stacki açar ve `producer`, `kafka`, `spark-streaming` loglarını takip eder.
