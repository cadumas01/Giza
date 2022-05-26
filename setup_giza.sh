#!/bin/bash

num=$1

if [[ ${num} == "" ]]; then
  echo "Please provide the node number."
  exit 1
fi

sed -i "s/\[NUM\]/$num/g" configs/giza/*

sudo cp configs/giza/cassandra.yaml /etc/cassandra/cassandra.yaml
sudo cp configs/giza/cassandra-rackdc.properties /etc/cassandra/cassandra-rackdc.properties

sudo systemctl stop cassandra.service
sudo rm -rf /var/lib/cassandra/*
sudo systemctl start cassandra.service

until $(nc -z "10.10.1.$num" 9042)
do
  sleep 1
done

cqlsh 192.168.1.$num -e "CREATE KEYSPACE giza WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};"

echo "setup_giza.sh has been called"