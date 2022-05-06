#!/bin/bash

sed -i "s/\[NUM\]/$1/g" configs/giza/*

sudo cp configs/giza/cassandra.yaml /etc/cassandra/cassandra.yaml
sudo cp configs/giza/cassandra-rackdc.properties /etc/cassandra/cassandra-rackdc.properties

sudo systemctl restart cassandra.service

sleep 5

cqlsh 192.168.1.$num -e "CREATE KEYSPACE giza WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};"
