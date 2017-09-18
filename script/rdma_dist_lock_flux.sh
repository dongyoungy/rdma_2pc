#!/bin/bash

job_name="rdma_dist_lock"
work_dir="/home/dyoon/temp/$job_name/dir"

for num_node in 4 8
do
  for num_user in 1
  do
    for workload in simple hotspot
    do
      num_core=`expr 1 + $num_user`
      resource_str="nodes=$num_node:ppn=$num_core,pmem=4gb,walltime=00:15:00"
      for duration in 600
      do
        wall_time=`expr 5 + $duration / 60`
        for num_lock_object in 10
        do
          for request_per_tx in 1
          do
            for lock_mode in remote-poll remote-queue
            do
              for think_time_type in zero normal
              do
                for se_rule in fail
                do
                  for es_rule in fail
                  do
                    for ee_rule in fail
                    do
                      for num_retry in 10000
                      do
                        let "job_counter++"
                        mkdir -p $work_dir$job_counter
                        qsub -A engin_flux \
                          -l "$resource_str" -N $job_name \
                          -q flux \
                          -e /home/dyoon/work/pbs/rdma/rdma-dist-lock-$lock_mode-$workload-$think_time_type-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-$se_rule-$es_rule-$ee_rule-${duration}s.e \
                          -o /home/dyoon/work/pbs/rdma/rdma-dist-lock-$lock_mode-$workload-$think_time_type-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-$se_rule-$es_rule-$ee_rule-${duration}s.o \
                          -v num_node=$num_node,work_dir=$work_dir,job_counter=$job_counter,duration=$duration,num_user=$num_user,num_lock_object=$num_lock_object,lock_mode=$lock_mode,se_rule=$se_rule,es_rule=$es_rule,ee_rule=$ee_rule,num_retry=$num_retry,request_per_tx=$request_per_tx,workload=$workload,think_time_type=$think_time_type \
                          rdma_dist_lock.pbs
                        sleep 10
                        num_waiting_jobs=`qstat -i -u dyoon 2>&1 | grep $job_name | wc -l`
                        num_running_jobs=`qstat -r -u dyoon 2>&1 | grep $job_name | wc -l`
                        num_jobs=$(($num_waiting_jobs+$num_running_jobs))
                        while [ $num_jobs -gt 0 ]
                        do
                          sleep 1
                          num_waiting_jobs=`qstat -i -u dyoon 2>&1 | grep $job_name | wc -l`
                          num_running_jobs=`qstat -r -u dyoon 2>&1 | grep $job_name | wc -l`
                          num_jobs=$(($num_waiting_jobs+$num_running_jobs))
                          wait_time=$((wait_time+1))
                        done
                      done
                    done
                  done
                done
              done
            done
          done
        done
      done
    done
  done
done

./notify_me.sh $job_name
