#!/bin/bash
gnome-terminal --tab -e "bash -c 'zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties;exec $SHELL'"
gnome-terminal --tab -e "bash -c 'cassandra -f;exec $SHELL'"
gnome-terminal --tab -e "bash -c 'sleep 5;kafka-server-start.sh $KAFKA_HOME/config/server.properties;exec $SHELL'"
gnome-terminal --tab -e "bash -c 'sleep 10;kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic weather;kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic forecast;cd producer;sbt run;exec $SHELL'"
gnome-terminal --tab -e "bash -c 'sleep 15;cd sparkstreaming;sbt run;exec $SHELL'"
gnome-terminal --tab -e "bash -c 'sleep 20;cd app;npm install;npm start;exec $SHELL'"
sleep 25
x=0
google-chrome http://localhost:14520 || firefox http://localhost:14520 || ( xdg-open http://localhost:14520 && x=1 )
echo $x
if [[ $x -eq 0 ]]
then
	kafka-server-start.sh
	zookeeper-server-start.sh
	kill $(pgrep bash)
	kill -9 $(pgrep bash)
fi
