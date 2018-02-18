#!/bin/bash

job_name="rdma_dist2"
work_dir="/proj/rdmaresearch-PG0/temp/$job_name/dir"
output_dir="/proj/rdmaresearch-PG0/work/output"
nodes_file="./nodes"

# for lock_mode in simple_retry d2lm_2 drtm ncosed traditional
#for num_node in 6 10 12 14 16 18
#for workload_name in long_0.1_50 long_0.01_50 tpcc-random-long-1_0.1 tpcc-random-long-1_0.01
#for workload_name in tpcc-random-long_1_0.01 tpcc-random-long_1_0.1 tpcc-random-long_1_0.25 tpcc-random-long_1_0.5
for workload_name in tpcc-random-long_1
do
  #for num_warehouse in 10
  #do
  #for exponent in 2 3
  #do
  #for shared in 0.00001 0.0001 0.001 0.01 0.1 0.5 1
  for shared in 0.001 0.01 0.1
  #for shared in 0.3
  do
    for fsrows in 100
    do
      for fstime in 200
      do
        workload=$workload_name\_$shared\_$fsrows\_$fstime
        #workload=$workload_name
        for duration in 60
        do
          for num_lock_object in 10000000
          do
            for request_per_tx in 1
            do
              for think_time_type in simple 
              do
                for think_time_duration in 0
                do
                  for num_retry in 10 
                  do
                    for random_backoff in true 
                    do
                      for base_backoff in 10
                      do
                        for max_backoff in 10000
                        do
                          for num_node in 32
                            #for lock_mode in d2lm_100000 simple_retry drtm traditional
                            #for lock_mode in simple_retry d2lm_2 drtm traditional
                          do
                            #for lock_mode in traditional traditional2_10 d2lm_10000000_0_T simple_retry drtm ncosed
                            for lock_mode in traditional2_10
                            #for lock_mode in d2lm_10000_0_T d2lm_10000 drtm ncosed simple_retry traditional
                            #for lock_mode in d2lm_10000 d2lm_10000_0_T
                            do
                              for read_lock_time in 0 
                              do
                                #for lease in 10 50 100 500 1000 5000 10000
                                #do
                                #for failrate in 0.001 0.01 0.1
                                #do
                                #lock_mode=$lock_mode_name\_$lease\_$failrate
                                #lock_mode=$lock_mode_name
                                user_num=$(($num_node / 2))
                                for num_user in 4
                                do
                                  let "job_counter++"
                                  mkdir -p $work_dir$job_counter
                                  echo "mpirun -pernode -np $num_node -f $nodes_file -wdir /proj/rdmaresearch-PG0/work/rdma_2pc/bin ./lock_simulation $work_dir$job_counter $num_lock_object $duration $request_per_tx $num_user $lock_mode $num_retry $workload $think_time_type $think_time_duration $random_backoff $base_off $max_backoff 1> $output_dir/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-${duration}s.o 2> $output_dir/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-bb_$base_backoff-mb_$max_backoff-${duration}s.e"
                                  mpirun --mca btl_openib_warn_nonexistent_if 0 --mca orte_base_help_aggregate 0 --mca btl ^tcp -pernode -np $num_node -hostfile $nodes_file -wdir /proj/rdmaresearch-PG0/work/rdma_2pc/bin ./lock_simulation $work_dir$job_counter $num_lock_object $duration $request_per_tx $num_user $lock_mode $num_retry $workload $think_time_type $think_time_duration $random_backoff $base_backoff $max_backoff $read_lock_time 1> $output_dir/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-bb$base_backoff-mb$max_backoff-rlt$read_lock_time-${duration}s.o 2> $output_dir/rdma-dist-lock-$lock_mode-$workload-$think_time_type-tt$think_time_duration-nn$num_node-user$num_user-obj$num_lock_object-req$request_per_tx-retry$num_retry-rb_$random_backoff-bb$base_backoff-mb$max_backoff-rlt$read_lock_time-${duration}s.e &
                                  sleep 60
                                  num_jobs=`ps -ef | grep mpirun | grep -v grep | wc -l`
                                  wait_time=0
                                  while [ $num_jobs -gt 0 ]
                                  do
                                    sleep 1
                                    num_jobs=`ps -ef | grep mpirun | grep -v grep | wc -l`
                                    wait_time=$((wait_time+1))
                                    if (( wait_time > $duration ))
                                    then
                                      killall mpirun
                                      sleep 10
                                    fi
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
        done
      done
    done
  done
done

./notify_me.sh $job_name
