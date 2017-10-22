#!/bin/bash

for node in $(cat nodes); do
  echo "Setting nofile in $node..."
  ssh -o "StrictHostKeyChecking no" -p 22 $node "echo '* soft nofile 1000000' | sudo tee --append /etc/security/limits.conf"
  ssh -o "StrictHostKeyChecking no" -p 22 $node "echo '* hard nofile 1000000' | sudo tee --append /etc/security/limits.conf"
done
wait
echo "DONE"
