#!/bin/bash

job_name="rdma_perf_test_flux"
work_dir="/home/dyoon/temp/$job_name/dir"

for num_node in 3 5 9
do
  for num_thread in 1
  do
    num_core=`expr 1 + $num_thread`
    resource_str="nodes=$num_node:ppn=$num_core,mem=100mb,walltime=00:10:00"
    for duration in 300
    do
      wall_time=`expr 5 + $duration / 60`
      for op in cas fa
      do
        echo "testing $op with $num_node nodes and $num_thread threads for $duration seconds with walltime of $wall_time..."
        let "job_counter++"
        mkdir -p $work_dir$job_counter
        qsub -A engin_flux \
          -l "$resource_str" -N $job_name \
          -q flux \
          -e /home/dyoon/work/pbs/rdma/microbench-$op-nn$num_node-th$num_thread-${duration}s.e \
          -o /home/dyoon/work/pbs/rdma/microbench-$op-nn$num_node-th$num_thread-${duration}s.o \
          -v work_dir=$work_dir,job_counter=$job_counter,op=$op,num_thread=$num_thread,duration=$duration \
          test_cs_fa.pbs
      done
    done
  done
done

num_jobs=`qstat -i -u dyoon 2>&1 | wc -l`
num_jobs=$(($num_jobs-1))
while [ $num_jobs -gt 0 ]
do
  sleep 1
  num_jobs=`qstat -i -u dyoon 2>&1 | wc -l`
  num_jobs=$((num_jobs-1))
  wait_time=$((wait_time+1))
done
./notify_me.sh $job_name
