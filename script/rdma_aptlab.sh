#!/bin/bash

job_name="rdma_dist2"
work_dir="/proj/rdmaresearch-PG0/temp/$job_name/dir"
output_dir="/proj/rdmaresearch-PG0/work/output"
nodes_file="./nodes"

# for lock_mode in simple_retry d2lm_2 drtm ncosed traditional
for num_node in 128 
do
  for num_user in 1
  do
    for workload in tpcc-hotspot
    do
      for duration in 180 300
      do
        for num_lock_object in 1
        do
          for request_per_tx in 1
          do
            for think_time_type in simple
            do
              for think_time_duration in 0 10 100
              do
                for num_retry in 10 100 1000 10000
                do
                  for random_backoff in true
                  do
                    for lock_mode in traditional d2lm_2 simple_retry drtm
                    #for lock_mode in simple_retry d2lm_2 drtm traditional
                    do
                      let "job_counter++"
                      mkdir -p $work_dir$job_counter
                      mpirun -np $num_node -f $nodes_file /proj/rdmaresearch-PG0/work/rdma_2pc/bin/lock_simulation $work_dir$job_counter $num_lock_object $duration $request_per_tx $num_user $lock_mode $num_retry $workload $think_time_type $think_time_duration $random_backoff 1> $output_dir/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-${duration}s.o 2> $output_dir/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-${duration}s.e 
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
