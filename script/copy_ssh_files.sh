#!/bin/bash

for node in $(cat nodes); do
  echo "copy ssh files to $node..."
  scp ~/.ssh/authorized_keys $node:~/.ssh
  scp ~/.ssh/known_hosts $node:~/.ssh
done
wait
echo "DONE"
