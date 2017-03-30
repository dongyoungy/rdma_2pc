#!/bin/bash

duration=60
simulate_tx_delay_min=10000
simulate_tx_delay_max=50000
#num_node=3
rand_seed=31671
#rand_seed=$RANDOM
workload_str="microbench"
wall_time=30
node_list="cn001 cn002 cn003 cn004 cn006 cn007 cn008 cn010 cn012 cn013 cn014 cn016 cn017 cn018 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn030 cn032 cn033 cn034 cn035 cn036 cn039 cn040"
#-m "cn004 cn005 cn006 cn008 cn010 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040" \
#-R "span[ptile=4] affinity[thread(2):cpubind=thread:distribute=balance]" -R rusage[mem=1024] -J local \
#-m "cn002 cn003 cn004 cn006 cn007 cn008 cn009 cn010 cn011 cn012 cn013 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn025 cn026 cn027 cn028 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn040" \
#-Q "all ~0" \

for num_node in 16
do
  for num_user in 8 16 32
  do 
    for simulate_tx_delay in 0
    do
      for num_manager_per_node in 1
      do
        for num_tx in 100000
        do
          for contention_index in 0.0001
          do
            for think_time in 0
            do

              #let lock_method=1 # proxy-queue
              #let sleep_time=100000
              #if [ $lock_method -eq 0 ]
              #then
                #lock_method_str="proxy-retry"
              #elif [ $lock_method -eq 1 ]
              #then
                #lock_method_str="proxy-queue"
              #elif [ $lock_method -eq 2 ]
              #then
                #lock_method_str="direct-poll"
              #elif [ $lock_method -eq 3 ]
              #then
                #lock_method_str="direct-notify"
              #elif [ $lock_method -eq 4 ]
              #then
                #lock_method_str="direct-queue"
              #fi
              #for min_backoff in 100
              #do
                #for max_backoff in 0
                #do
                  #if [ $max_backoff -eq 0 ]
                  #then
                    #if [ $min_backoff -ne 100 ]
                    #then
                      #continue
                    #fi
                  #fi
                  #for num_retry in 3
                  #do
                    #for s_e_rule in 0
                    #do
                      #for e_s_rule in 0
                      #do
                        #for e_e_rule in 0
                        #do
                          #if [ $s_e_rule -eq 0 ]
                          #then
                            #s_e_str="fail"
                          #elif [ $s_e_rule -eq 1 ]
                          #then
                            #s_e_str="poll"
                          #fi

                          #if [ $e_s_rule -eq 0 ]
                          #then
                            #e_s_str="fail"
                          #elif [ $e_s_rule -eq 1 ]
                          #then
                            #e_s_str="poll"
                          #elif [ $e_s_rule -eq 2 ]
                          #then
                            #e_s_str="queue"
                          #fi

                          #if [ $e_e_rule -eq 0 ]
                          #then
                            #e_e_str="fail"
                          #elif [ $e_e_rule -eq 1 ]
                          #then
                            #e_e_str="poll"
                          #elif [ $e_e_rule -eq 2 ]
                          #then
                            #e_e_str="queue"
                          #fi

                          #bsub -n $num_node -a openmpi -W $wall_time \
                            #-R "span[ptile=1] affinity[core(10)]" -R rusage[mem=8192] -J local \
                            #-m "$node_list" \
                            #-eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                            #-oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                            #"sh mb_test.lsf $num_manager_per_node $num_tx $contention_index $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $sleep_time $think_time $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $min_backoff $max_backoff $rand_seed"
                          #num_jobs=`bjobs 2>&1 | wc -l`
                          #num_jobs=$(($num_jobs-1))
                          #wait_time=0
                          #while [ $num_jobs -gt 0 ]
                          #do
                            #sleep 1
                            #num_jobs=`bjobs 2>&1 | wc -l`
                            #num_jobs=$((num_jobs-1))
                            #wait_time=$((wait_time+1))
                            ##if (( wait_time > 660 ))
                            ##then
                            ##  bkill 0
                            ##fi
                          #done
                        #done
                      #done
                    #done
                  #done
                #done
              #done



              #let lock_method=2 # direct-poll
              #let sleep_time=0
              #if [ $lock_method -eq 0 ]
              #then
                #lock_method_str="proxy-retry"
              #elif [ $lock_method -eq 1 ]
              #then
                #lock_method_str="proxy-queue"
              #elif [ $lock_method -eq 2 ]
              #then
                #lock_method_str="direct-poll"
              #elif [ $lock_method -eq 3 ]
              #then
                #lock_method_str="direct-notify"
              #elif [ $lock_method -eq 4 ]
              #then
                #lock_method_str="direct-queue"
              #fi
              #for min_backoff in 100
              #do
                #for max_backoff in 0 100000
                #do
                  #if [ $max_backoff -eq 0 ]
                  #then
                    #if [ $min_backoff -ne 100 ]
                    #then
                      #continue
                    #fi
                  #fi
                  #for num_retry in 3 10
                  #do
                    #for s_e_rule in 0
                    #do
                      #for e_s_rule in 0 1
                      #do
                        #for e_e_rule in 0
                        #do
                          #if [ $s_e_rule -eq 0 ]
                          #then
                            #s_e_str="fail"
                          #elif [ $s_e_rule -eq 1 ]
                          #then
                            #s_e_str="poll"
                          #fi

                          #if [ $e_s_rule -eq 0 ]
                          #then
                            #e_s_str="fail"
                          #elif [ $e_s_rule -eq 1 ]
                          #then
                            #e_s_str="poll"
                          #elif [ $e_s_rule -eq 2 ]
                          #then
                            #e_s_str="queue"
                          #fi

                          #if [ $e_e_rule -eq 0 ]
                          #then
                            #e_e_str="fail"
                          #elif [ $e_e_rule -eq 1 ]
                          #then
                            #e_e_str="poll"
                          #elif [ $e_e_rule -eq 2 ]
                          #then
                            #e_e_str="queue"
                          #fi

                          #bsub -n $num_node -a openmpi -W $wall_time \
                            #-R "span[ptile=1] affinity[core(10)]" -R rusage[mem=8192] -J local \
                            #-m "$node_list" \
                            #-eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                            #-oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                            #"sh mb_test.lsf $num_manager_per_node $num_tx $contention_index $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $sleep_time $think_time $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $min_backoff $max_backoff $rand_seed"
                          #num_jobs=`bjobs 2>&1 | wc -l`
                          #num_jobs=$(($num_jobs-1))
                          #wait_time=0
                          #while [ $num_jobs -gt 0 ]
                          #do
                            #sleep 1
                            #num_jobs=`bjobs 2>&1 | wc -l`
                            #num_jobs=$((num_jobs-1))
                            #wait_time=$((wait_time+1))
                            ##if (( wait_time > 660 ))
                            ##then
                            ##  bkill 0
                            ##fi
                          #done
                        #done
                      #done
                    #done
                  #done
                #done
              #done


              #let lock_method=3 # direct-notify
              #let sleep_time=10000
              #if [ $lock_method -eq 0 ]
              #then
                #lock_method_str="proxy-retry"
              #elif [ $lock_method -eq 1 ]
              #then
                #lock_method_str="proxy-queue"
              #elif [ $lock_method -eq 2 ]
              #then
                #lock_method_str="direct-poll"
              #elif [ $lock_method -eq 3 ]
              #then
                #lock_method_str="direct-notify"
              #elif [ $lock_method -eq 4 ]
              #then
                #lock_method_str="direct-queue"
              #fi
              #for min_backoff in 100
              #do
                #for max_backoff in 0 100000
                #do
                  #if [ $max_backoff -eq 0 ]
                  #then
                    #if [ $min_backoff -ne 100 ]
                    #then
                      #continue
                    #fi
                  #fi
                  #for num_retry in 3
                  #do
                    #for s_e_rule in 0
                    #do
                      #for e_s_rule in 0
                      #do
                        #for e_e_rule in 0
                        #do
                          #if [ $s_e_rule -eq 0 ]
                          #then
                            #s_e_str="fail"
                          #elif [ $s_e_rule -eq 1 ]
                          #then
                            #s_e_str="poll"
                          #fi

                          #if [ $e_s_rule -eq 0 ]
                          #then
                            #e_s_str="fail"
                          #elif [ $e_s_rule -eq 1 ]
                          #then
                            #e_s_str="poll"
                          #elif [ $e_s_rule -eq 2 ]
                          #then
                            #e_s_str="queue"
                          #fi

                          #if [ $e_e_rule -eq 0 ]
                          #then
                            #e_e_str="fail"
                          #elif [ $e_e_rule -eq 1 ]
                          #then
                            #e_e_str="poll"
                          #elif [ $e_e_rule -eq 2 ]
                          #then
                            #e_e_str="queue"
                          #fi

                          #bsub -n $num_node -a openmpi -W $wall_time \
                            #-R "span[ptile=1] affinity[core(10)]" -R rusage[mem=8192] -J local \
                            #-m "$node_list" \
                            #-eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                            #-oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                            #"sh mb_test.lsf $num_manager_per_node $num_tx $contention_index $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $sleep_time $think_time $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $min_backoff $max_backoff $rand_seed"
                          #num_jobs=`bjobs 2>&1 | wc -l`
                          #num_jobs=$(($num_jobs-1))
                          #wait_time=0
                          #while [ $num_jobs -gt 0 ]
                          #do
                            #sleep 1
                            #num_jobs=`bjobs 2>&1 | wc -l`
                            #num_jobs=$((num_jobs-1))
                            #wait_time=$((wait_time+1))
                            ##if (( wait_time > 660 ))
                            ##then
                            ##  bkill 0
                            ##fi
                          #done
                        #done
                      #done
                    #done
                  #done
                #done
              #done

              # direct-queue
              let lock_method=4
              let sleep_time=0
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
              elif [ $lock_method -eq 4 ]
              then
                lock_method_str="direct-queue"
              fi
              for min_backoff in 100
              do
                for max_backoff in 100000
                do
                  if [ $max_backoff -eq 0 ]
                  then
                    if [ $min_backoff -ne 100 ]
                    then
                      continue
                    fi
                  fi
                  for num_retry in 3
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

                          bsub -n $num_node -a openmpi -W $wall_time \
                            -R "span[ptile=1] affinity[core(10)]" -R rusage[mem=8192] -J local \
                            -m "$node_list" \
                            -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                            -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/sosp2017/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-st$sleep_time-tt$think_time-bmin$min_backoff-bmax$max_backoff-nn$num_node-mpn$num_manager_per_node-u$num_user-tx$num_tx-ci$contention_index-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                            "sh mb_test.lsf $num_manager_per_node $num_tx $contention_index $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $sleep_time $think_time $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $min_backoff $max_backoff $rand_seed"
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
    done
  done
done
