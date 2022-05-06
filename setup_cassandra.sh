#!/bin/bash

sed -i "s/\[NUM\]/$1/g" configs/cassandra/*

sudo cp configs/cassandra/cassandra.yaml /etc/cassandra/cassandra.yaml
sudo cp configs/cassandra/cassandra-rackdc.properties /etc/cassandra/cassandra-rackdc.properties

sudo systemctl restart cassandra.service

sleep 5

cqlsh 192.168.1.$num -e "CREATE KEYSPACE cassandra WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor': 1};"
