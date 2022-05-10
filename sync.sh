#!/bin/bash

servers="./servers.txt"

while IFS= read -r line
do
  port=${line% *}
  server=${line##* }

  rsync --rsh="$port" -avP --exclude=.git --exclude results . $server:~/giza/ &
done < "$servers"

wait $(jobs -p)
