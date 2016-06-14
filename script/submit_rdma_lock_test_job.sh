#!/bin/bash

for nn in {17..19}
do
  bsub -n $nn < ./rdma_dist_lock_local.lsf
  num_jobs=`bjobs 2>&1 | wc -l`
  num_jobs=$(($num_jobs-1))
  while [ $num_jobs -gt 0 ]
  do
    sleep 1
    num_jobs=`bjobs 2>&1 | wc -l`
    num_jobs=$((num_jobs-1))
  done
done

for nn in {4..20}
do
  bsub -n $nn < ./rdma_dist_lock_remote.lsf
  num_jobs=`bjobs 2>&1 | wc -l`
  num_jobs=$(($num_jobs-1))
  while [ $num_jobs -gt 0 ]
  do
    sleep 1
    num_jobs=`bjobs 2>&1 | wc -l`
    num_jobs=$((num_jobs-1))
  done
done
