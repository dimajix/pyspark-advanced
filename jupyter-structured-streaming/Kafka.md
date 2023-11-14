# Start Kafka
sudo systemctl start kafka

# Create Messages
/opt/kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic lala

# Consume Messageskaf   
/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic lala \
    --from-beginning

# List Topics
/opt/kafka/bin/kafka-topics.sh \
    --zookeeper localhost:2181 \
    --list


# Twitter Data
./s3cat.py s3://dimajix-training/data/twitter-sample/ \
   | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic twitter

/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic twitter \
    --from-beginning

/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --topic twitter --delete


# Weather Data
./s3cat.py s3://dimajix-training/data/weather-sample/ \
   | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic weather

/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic weather \
    --from-beginning

/opt/kafka/bin/kafka-topics.sh \
    --zookeeper localhost:2181 \
    --topic weather \
    --delete
