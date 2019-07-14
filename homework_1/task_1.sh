#!/bin/bash
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper 10.156.0.3:2181,10.156.0.4:2181,10.156.0.5:2181 --topic dborisov --partitions 3 --replication-factor 3
