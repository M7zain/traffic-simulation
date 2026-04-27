her şeyi durdurmak için 

docker compose down

Image build yapıp çalıştırmak için ------ > buradan başla 

docker compose up -d --build

çalışan servisleri görmek için 

docker compose ps

3 servisi aynı anda çıktılarını takip etmek için bunu çalıştır 

docker compose logs -f producer kafka spark-streaming


producerdan kafkaya gelen verilerin geldiğini görmek için 

docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic traffic-topic


spark çıktılarını görmek için 

docker compose logs -f spark-streaming

yada localhost:8080 den kontrol edebilirsin

mongodb de saklanan verilerin sayisini kontrol etmek icin 

docker compose exec mongo mongosh --quiet --eval "db.traffic.countDocuments()" traffic_db
