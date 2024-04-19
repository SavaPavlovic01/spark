cd C:/kafka/kafka_2.13-3.7.0
bash .\bin\kafka-topics.sh --bootstrap-server localhost:9092 --topic users --create --partitions 3 --replication-factor 1
bash .\bin\kafka-topics.sh --bootstrap-server localhost:9092 --topic usersSecond --create --partitions 3 --replication-factor 1
cd C:\Users\ps200536d\Desktop\projekti\spark