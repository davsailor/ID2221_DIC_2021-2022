# ID2221_DIC_2021-2022
Final project of Data Intensive Computing at KTH, accademic year 2021-2022

## Command to start Zookeeper server
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

## Command to start Kafka server
kafka-server-start.sh $KAFKA_HOME/config/server.properties

## Command to create the topic "weather"
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic weather
