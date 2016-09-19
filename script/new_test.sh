#!/bin/bash

# running mixed
workload_type="0"
#local_workload_ratio="0.5"
duration=60
simulate_tx_delay_min=10000
simulate_tx_delay_max=50000
num_user=1
rand_seed=31671
#rand_seed=$RANDOM
if [ $workload_type -eq 0 ]
then
  workload_str="uniform"
elif [ $workload_type -eq 1 ]
then
  workload_str="hotspot"
elif [ $workload_type -eq 2 ]
then
  workload_str="all_local"
elif [ $workload_type -eq 3 ]
then
  workload_str="mixed"
fi

for num_node in 20
do 
  for simulate_tx_delay in 0
  do
    for lock_method in 1 # 0 = proxy, 1 = direct
    do
      if [ $lock_method -eq 0 ]
      then
        lock_method_str="proxy"
      elif [ $lock_method -eq 1 ]
      then
        lock_method_str="direct"
      fi
      for num_lock_objects in 64 512
      do
        for num_request_per_tx in 2 5 10
        do
          let num_tx=500000/num_request_per_tx
          for local_workload_ratio in 0.5
          do
            for shared_lock_ratio in 0.5
            do
              for max_backoff in 0 1000
              do
                for num_retry in 1 3 5
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

                        bsub -n $num_node -a openmpi -W 10 -m "cn004 cn005 cn006 cn008 cn010 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040" -R "span[ptile=1] affinity[core(1)]" -R rusage[mem=1024] -J local \
                          -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-bo$max_backoff-nn$num_node-u$num_user-lo$num_lock_objects-tx$num_tx-rq$num_request_per_tx-sr$shared_lock_ratio-local$local_workload_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e \
                          -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-$workload_str-$s_e_str-$e_s_str-$e_e_str-fr$num_retry-pr$num_retry-bo$max_backoff-nn$num_node-u$num_user-lo$num_lock_objects-tx$num_tx-rq$num_request_per_tx-sr$shared_lock_ratio-local$local_workload_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal \
                          "sh new_test.lsf $num_lock_objects $num_tx $num_request_per_tx $num_user $lock_method $s_e_rule $e_s_rule $e_e_rule $num_retry $num_retry $workload_type $local_workload_ratio $shared_lock_ratio $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max $max_backoff $rand_seed"
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


# running mixed (custom)
#workload_type="3"
##local_workload_ratio="0.5"
#duration=60
#simulate_tx_delay_min=10000
#simulate_tx_delay_max=50000
#num_user=1
#for num_node in 20
#do 
  #for simulate_tx_delay in 0
  #do
    #for lock_method in 0 # 0 = proxy, 1 = direct
    #do
      #if [ $lock_method -eq 0 ]
      #then
        #lock_method_str="proxy"
      #elif [ $lock_method -eq 1 ]
      #then
        #lock_method_str="direct"
      #fi
      #for num_lock_objects in 16
      #do
        #for num_requests in 5
        #do
          #for local_workload_ratio in 1
          #do
            #for shared_lock_ratio in 0.25 0.5 0.75 1
            #do
              #bsub -n $num_node -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-mixed-nn$num_node-user$num_user-lo$num_lock_objects-rq$num_requests-sr$shared_lock_ratio-local$local_workload_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-mixed-nn$num_node-user$num_user-lo$num_lock_objects-rq$num_requests-sr$shared_lock_ratio-local$local_workload_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal "sh rdma_dist_lock.lsf $num_lock_objects $num_requests $num_user $lock_method $workload_type $local_workload_ratio $shared_lock_ratio $duration $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max"
              #num_jobs=`bjobs 2>&1 | wc -l`
              #num_jobs=$(($num_jobs-1))
              #wait_time=0
              #while [ $num_jobs -gt 0 ]
              #do
                #sleep 1
                #num_jobs=`bjobs 2>&1 | wc -l`
                #num_jobs=$((num_jobs-1))
                #wait_time=$((wait_time+1))
                #if (( wait_time > 300 ))
                #then
                  #bkill 0
                #fi
              #done
            #done
          #done
        #done
      #done
    #done
  #done
#done

#for nn in 20
#do
  #for r in 0 0.25 0.5 0.75 1
  #do
    #bsub -n $nn -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_local/uniform_shared_lock_$r.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_local/uniform_shared_lock_$r.o -q normal "sh rdma_dist_lock_local.lsf 1 $r"
    #num_jobs=`bjobs 2>&1 | wc -l`
    #num_jobs=$(($num_jobs-1))
    #while [ $num_jobs -gt 0 ]
    #do
      #sleep 1
      #num_jobs=`bjobs 2>&1 | wc -l`
      #num_jobs=$((num_jobs-1))
    #done
  #done
#done

#for nn in 20
#do
  #for r in 0 0.25 0.5 0.75 1
  #do
    #bsub -n $nn -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_remote/uniform_shared_lock_$r.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_remote/uniform_shared_lock_$r.o -q normal "sh rdma_dist_lock_remote.lsf 1 $r"
    #num_jobs=`bjobs 2>&1 | wc -l`
    #num_jobs=$(($num_jobs-1))
    #while [ $num_jobs -gt 0 ]
    #do
      #sleep 1
      #num_jobs=`bjobs 2>&1 | wc -l`
      #num_jobs=$((num_jobs-1))
    #done
  #done
#done

#for nn in 20
#do
  ##for r in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1
  #for r in 0.35 0.95
  #do
    #bsub -n $nn -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_local/mixed_$r.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_local/mixed_$r.o -q normal "sh rdma_dist_lock_local.lsf $r"
    #num_jobs=`bjobs 2>&1 | wc -l`
    #num_jobs=$(($num_jobs-1))
    #while [ $num_jobs -gt 0 ]
    #do
      #sleep 1
      #num_jobs=`bjobs 2>&1 | wc -l`
      #num_jobs=$((num_jobs-1))
    #done
  #done
#done

#for nn in 20
#do
  ##for r in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1
  #for r in 0.35 0.45
  #do
    #bsub -n $nn -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_remote/mixed_$r.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_remote/mixed_$r.o -q normal "sh rdma_dist_lock_remote.lsf $r"
    #num_jobs=`bjobs 2>&1 | wc -l`
    #num_jobs=$(($num_jobs-1))
    #while [ $num_jobs -gt 0 ]
    #do
      #sleep 1
      #num_jobs=`bjobs 2>&1 | wc -l`
      #num_jobs=$((num_jobs-1))
    #done
  #done
#done

#for nn in {1..1}
#for nn in 20
#do
  #bsub -n $nn < "sh ./rdma_dist_lock_local.lsf $1"
  #num_jobs=`bjobs 2>&1 | wc -l`
  #num_jobs=$(($num_jobs-1))
  #while [ $num_jobs -gt 0 ]
  #do
    #sleep 1
    #num_jobs=`bjobs 2>&1 | wc -l`
    #num_jobs=$((num_jobs-1))
  #done
#done

#for nn in {1..1}
#for nn in 20
#do
  #bsub -n $nn < ./rdma_dist_lock_remote.lsf
  #num_jobs=`bjobs 2>&1 | wc -l`
  #num_jobs=$(($num_jobs-1))
  #while [ $num_jobs -gt 0 ]
  #do
    #sleep 1
    #num_jobs=`bjobs 2>&1 | wc -l`
    #num_jobs=$((num_jobs-1))
  #done
#done
