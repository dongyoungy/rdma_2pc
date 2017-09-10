#!/bin/bash

#num_node=$1
node_list="cn001 cn002 cn003 cn004 cn005 cn006 cn007 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn017 cn018 cn020 cn021 cn022 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn038 cn039 cn040 cn042 cn043"
job_name="rdma_perf_test"
work_dir="/gpfs/gpfs0/groups/mozafari/dyoon/work/temp2/$job_name/dir"
job_queue=experiments
#job_queue=normal

for num_node in 3 5 9 
do
  for num_thread in 1
  do
    num_core=`expr 1 + $num_thread`
    resource_str="span[ptile=1] affinity[core($num_core)] rusage[mem=8192]"
    for duration in 300
    do
      wall_time=`expr 5 + $duration / 60`
      for op in cas fa
      do
        echo "testing $op with $num_node nodes and $num_thread threads for $duration seconds with walltime of $wall_time..."
        let "job_counter++"
        mkdir -p $work_dir$job_counter
        bsub -n $num_node -a openmpi -W $wall_time \
          -R "$resource_str" -J $job_name \
          -q $job_queue \
          -m "$node_list" \
          -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_perf_test/microbench-$op-nn$num_node-th$num_thread-${duration}s.e \
          -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_perf_test/microbench-$op-nn$num_node-th$num_thread-${duration}s.o \
          "sh test_cs_fa.lsf $work_dir$job_counter $op $num_thread $duration"
      done
    done
  done
done

num_jobs=`bjobs -J $job_name 2>&1 | wc -l`
num_jobs=$(($num_jobs-1))
while [ $num_jobs -gt 0 ]
do
  sleep 1
  num_jobs=`bjobs -J $job_name 2>&1 | wc -l`
  num_jobs=$((num_jobs-1))
  wait_time=$((wait_time+1))
done
./notify_me.sh $job_name
