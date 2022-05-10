#!/bin/bash

num=$1

if [[ ${num} == "" ]]; then
  echo "Please provide the node number."
  exit 1
fi

sed -i "s/\[NUM\]/$num/g" configs/cassandra/*

sudo cp configs/cassandra/cassandra.yaml /etc/cassandra/cassandra.yaml
sudo cp configs/cassandra/cassandra-rackdc.properties /etc/cassandra/cassandra-rackdc.properties

sudo systemctl stop cassandra.service
sudo rm -rf /var/lib/cassandra/*
sudo systemctl start cassandra.service

until $(nc -z "192.168.1.$num" 9042)
do
  sleep 1
done

cqlsh 192.168.1.$num -e "CREATE KEYSPACE cassandra WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor': 1};"
