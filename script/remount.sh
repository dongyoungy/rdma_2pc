#!/bin/bash

for node in $(cat nodes); do
  echo "remounting NFS storage in $node..."
  ssh -o "StrictHostKeyChecking no" -p 22 $node "sudo umount -l /proj/rdmaresearch-PG0 && sudo mount ops.apt.emulab.net:/proj/rdmaresearch-PG0 /proj/rdmaresearch-PG0" &
done
wait
echo "DONE"
