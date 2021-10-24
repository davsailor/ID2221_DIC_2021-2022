########################################################################################
########################################################################################
######################################            ######################################
##################################                    ##################################
#############################       CURRENT CORRECT!       #############################
##################################                    ##################################
######################################            ######################################
########################################################################################
########################################################################################


#!/bin/bash

# start the zookeeper server for kafka
gnome-terminal --tab -e "bash -c 'zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties;exec $SHELL'"

# start the cassandra in foreground
gnome-terminal --tab -e "bash -c 'cassandra -f;exec $SHELL'"

# start the kafka server
gnome-terminal --tab -e "bash -c 'sleep 5;kafka-server-start.sh $KAFKA_HOME/config/server.properties;exec $SHELL'"

# add the topics 'weather' and 'forecast'
# start the kafka producer script
gnome-terminal --tab -e "bash -c 'sleep 10;kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic weather;kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic forecast;cd producer;sbt run;exec $SHELL'"

# start the kafka-sparkstreaming-cassandra-mllib script (core of the application)
gnome-terminal --tab -e "bash -c 'sleep 15;cd sparkstreaming;sbt run;exec $SHELL'"

# start the nodejs server
gnome-terminal --tab -e "bash -c 'sleep 20;cd app;npm install;npm start;exec $SHELL'"

# sleep calls are needed to let servers correctly boot


########################################################################################
########################################################################################
##################################                    ##################################
#########################                                      #########################
######################      GO TO localhost:14520 AND ENJOY      #######################
#########################                                      #########################
##################################                    ##################################
########################################################################################
########################################################################################
