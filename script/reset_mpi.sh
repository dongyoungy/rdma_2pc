#!/bin/bash

for node in $(cat nodes); do
  echo "killing MPI processes in $node..."
  ssh -o "StrictHostKeyChecking no" -p 22 $node "killall mpirun" &
  pids=`ssh -o 'StrictHostKeyChecking no' -p 22 $node ps -ef | grep hydra | awk '{print $2}'`
  for pid in $pids; do
    echo $pid
    ssh -o "StrictHostKeyChecking no" -p 22 $node "kill -9 $pid"  &
  done
done
wait
echo "DONE"
