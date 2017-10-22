#!/bin/bash

for node in $(cat nodes); do
  echo "installing openmpi in $node..."
  ssh -o "StrictHostKeyChecking no" -p 22 $node "sudo apt-get -y --force-yes install openmpi-bin libopenmpi-dev" &
done
wait
echo "DONE"
