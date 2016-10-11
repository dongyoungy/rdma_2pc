#!/bin/bash

duration=60
simulate_tx_delay_min=10000
simulate_tx_delay_max=50000
num_user=1
rand_seed=31671
#rand_seed=$RANDOM
workload_str="tpcc"

for num_node in 10
do 
  for simulate_tx_delay in 0
  do
    for lock_method in 2
    do
      if [ $lock_method -eq 0 ]
      then
        lock_method_str="proxy-retry"
      elif [ $lock_method -eq 1 ]
      then
        lock_method_str="proxy-queue"
      elif [ $lock_method -eq 2 ]
      then
        lock_method_str="direct-poll"
      elif [ $lock_method -eq 3 ]
      then
        lock_method_str="direct-notify"
      fi
      for num_tx in 100000
      do
        for min_backoff in 1000
        do
          for max_backoff in 0 100000
          do
            for num_retry in 3 10
            do
              for s_e_rule in 0 1 
              do
                for e_s_rule in 0 1 2
                do
                  for e_e_rule in 0 1 2
                  do
                    if [ $s_e_rule -eq 0 ]
                    then
                      s_e_str="fail"
                    elif [ $s_e_rule -eq 1 ]
                    then
                      s_e_str="poll"
                    fi

                    if [ $e_s_rule -eq 0 ]
                    then
                      e_s_str="fail"
                    elif [ $e_s_rule -eq 1 ]
                    then
                      e_s_str="poll"
                    elif [ $e_s_rule -eq 2 ]
                    then
                      e_s_str="queue"
                    fi

                    if [ $e_e_rule -eq 0 ]
                    then
                      e_e_str="fail"
                    elif [ $e_e_rule -eq 1 ]
                    then
                      e_e_str="poll"
                    elif [ $e_e_rule -eq 2 ]
                    then
                      e_e_str="queue"
                    fi

                    #-m "cn004 cn005 cn006 cn008 cn010 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040" \
                      #-Q "all ~0" \
                      bsub -n $num_node -a openmpi -W 3 \
                      -R "span[ptile=1] affinity[core(4)]" -R rusage[mem=2048] -J local \
                      -m "cn001 cn002 cn004 cn005 cn006 cn008 cn010 cn012 cn013 cn014 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn040" \
                      -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-bmin$min_backoff-bmax$max_backoff-nn$num_node-u$num_user-tx$num_tx-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                      -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-bmin$min_backoff-bmax$max_backoff-nn$num_node-u$num_user-tx$num_tx-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                      "sh tpcc_test.lsf $num_tx $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $min_backoff $max_backoff $rand_seed"
                    num_jobs=`bjobs 2>&1 | wc -l`
                    num_jobs=$(($num_jobs-1))
                    wait_time=0
                    while [ $num_jobs -gt 0 ]
                    do
                      sleep 1
                      num_jobs=`bjobs 2>&1 | wc -l`
                      num_jobs=$((num_jobs-1))
                      wait_time=$((wait_time+1))
                      #if (( wait_time > 660 ))
                      #then
                      #  bkill 0
                      #fi
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

for num_node in 10
do 
  for simulate_tx_delay in 0
  do
    for lock_method in 1
    do
      if [ $lock_method -eq 0 ]
      then
        lock_method_str="proxy-retry"
      elif [ $lock_method -eq 1 ]
      then
        lock_method_str="proxy-queue"
      elif [ $lock_method -eq 2 ]
      then
        lock_method_str="direct-poll"
      elif [ $lock_method -eq 3 ]
      then
        lock_method_str="direct-notify"
      fi
      for num_tx in 100000
      do
        for min_backoff in 1000
        do
          for max_backoff in 0 100000
          do
            for num_retry in 3 10
            do
              for s_e_rule in 0 
              do
                for e_s_rule in 0
                do
                  for e_e_rule in 0 
                  do
                    if [ $s_e_rule -eq 0 ]
                    then
                      s_e_str="fail"
                    elif [ $s_e_rule -eq 1 ]
                    then
                      s_e_str="poll"
                    fi

                    if [ $e_s_rule -eq 0 ]
                    then
                      e_s_str="fail"
                    elif [ $e_s_rule -eq 1 ]
                    then
                      e_s_str="poll"
                    elif [ $e_s_rule -eq 2 ]
                    then
                      e_s_str="queue"
                    fi

                    if [ $e_e_rule -eq 0 ]
                    then
                      e_e_str="fail"
                    elif [ $e_e_rule -eq 1 ]
                    then
                      e_e_str="poll"
                    elif [ $e_e_rule -eq 2 ]
                    then
                      e_e_str="queue"
                    fi

                    #-m "cn004 cn005 cn006 cn008 cn010 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040" \
                      #-Q "all ~0" \
                      bsub -n $num_node -a openmpi -W 10 \
                      -R "span[ptile=1] affinity[core(4)]" -R rusage[mem=2048] -J local \
                      -m "cn001 cn002 cn004 cn005 cn006 cn008 cn010 cn012 cn013 cn014 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn040" \
                      -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-bmin$min_backoff-bmax$max_backoff-nn$num_node-u$num_user-tx$num_tx-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                      -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-bmin$min_backoff-bmax$max_backoff-nn$num_node-u$num_user-tx$num_tx-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                      "sh tpcc_test.lsf $num_tx $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $min_backoff $max_backoff $rand_seed"
                    num_jobs=`bjobs 2>&1 | wc -l`
                    num_jobs=$(($num_jobs-1))
                    wait_time=0
                    while [ $num_jobs -gt 0 ]
                    do
                      sleep 1
                      num_jobs=`bjobs 2>&1 | wc -l`
                      num_jobs=$((num_jobs-1))
                      wait_time=$((wait_time+1))
                      #if (( wait_time > 660 ))
                      #then
                      #  bkill 0
                      #fi
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

