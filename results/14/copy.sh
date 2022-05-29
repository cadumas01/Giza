#!/bin/bash

servers="../../servers.txt"
i=1

while IFS= read -r line
do
  port=${line% *}
  server=${line##* }

  rsync --rsh="$port" $server:~/Giza/latency.txt ./latency.$i.txt &
  rsync --rsh="$port" $server:~/Giza/lattput.txt ./lattput.$i.txt &
  i=$((i+1))
done < "$servers"

wait $(jobs -p)
