#!/bin/bash

for node in $(cat nodes); do
  echo "$node:"
  ssh -o "StrictHostKeyChecking no" -p 22 $node "ifconfig"
done
wait
echo "DONE"
