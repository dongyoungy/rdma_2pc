#!/bin/bash

job_name="rdma_dist2"
work_dir="/home/dyoon/temp/$job_name/dir"

# for lock_mode in simple_retry d2lm_2 drtm ncosed traditional
for num_node in 16 
do
  for num_user in 1
  do
    for workload in hotspot
    do
      #num_core=`expr 1 + $num_user`
      resource_str="qos=flux,procs=$num_node,tpn=1,pmem=1gb,walltime=00:15:00"
      for duration in 600
      do
        #wall_time=`expr 5 + $duration / 60`
        for num_lock_object in 1 10 100 1000
        do
          for request_per_tx in 1
          do
            for think_time_type in simple
            do
              for think_time_duration in 0 10 100 
              do
                for num_retry in 10 100 1000 10000 100000
                do
                  for random_backoff in true false
                  do
                    for lock_mode in simple_retry d2lm_2 drtm traditional
                    do
                      let "job_counter++"
                      mkdir -p $work_dir$job_counter
                      qsub -A mozafari_flux \
                        -l "$resource_str" -N $job_name \
                        -q flux \
                        -e /home/dyoon/work/pbs/rdma/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-${duration}s.e \
                        -o /home/dyoon/work/pbs/rdma/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-${duration}s.o \
                        -v num_node=$num_node,work_dir=$work_dir,job_counter=$job_counter,duration=$duration,num_user=$num_user,num_lock_object=$num_lock_object,lock_mode=$lock_mode,se_rule=$se_rule,es_rule=$es_rule,ee_rule=$ee_rule,num_retry=$num_retry,request_per_tx=$request_per_tx,workload=$workload,think_time_type=$think_time_type,random_backoff=$random_backoff,think_time_duration=$think_time_duration \
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

./notify_me.sh $job_name
