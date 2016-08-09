#!/bin/bash

# request size script
#rs=10
#bsub -n 20 -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_local/rs_$rs.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_local/rs_$rs.o -q normal "sh rdma_dist_lock_local.lsf 1"
#num_jobs=`bjobs 2>&1 | wc -l`
#num_jobs=$(($num_jobs-1))
#while [ $num_jobs -gt 0 ]
#do
  #sleep 1
  #num_jobs=`bjobs 2>&1 | wc -l`
  #num_jobs=$((num_jobs-1))
#done
#bsub -n 20 -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_remote/rs_$rs.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock_remote/rs_$rs.o -q normal "sh rdma_dist_lock_remote.lsf 1"

# running hotspot
workload_type="1"
duration=60
#simulate_tx_delay=0
simulate_tx_delay_min=10000
simulate_tx_delay_max=50000
num_user=2
for num_node in 20
do 
  local_workload_ratio="0.5"
  for simulate_tx_delay in 0
  do
    for lock_method in 0 1 # 0 = proxy, 1 = direct
    do
      if [ $lock_method -eq 0 ]
      then
        lock_method_str="proxy"
      elif [ $lock_method -eq 1 ]
      then
        lock_method_str="direct"
      fi
      for num_lock_objects in 512
      do
        for num_requests in 5
        do
          for shared_lock_ratio in 0 0.25 0.5 0.75 1
          do
            bsub -n $num_node -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-hotspot-nn$num_node-lo$num_lock_objects-rq$num_requests-sr$shared_lock_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-hotspot-nn$num_node-lo$num_lock_objects-rq$num_requests-sr$shared_lock_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal "sh rdma_dist_lock.lsf $num_lock_objects $num_requests $num_user $lock_method $workload_type $local_workload_ratio $shared_lock_ratio $duration $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max"
            num_jobs=`bjobs 2>&1 | wc -l`
            num_jobs=$(($num_jobs-1))
            wait_time=0
            while [ $num_jobs -gt 0 ]
            do
              sleep 1
              num_jobs=`bjobs 2>&1 | wc -l`
              num_jobs=$((num_jobs-1))
              wait_time=$((wait_time+1))
              if (( wait_time > 300 ))
              then
                bkill 0
              fi
            done
          done
        done
      done
    done
  done
done

# running mixed
#workload_type="3"
##local_workload_ratio="0.5"
#duration=60
#simulate_tx_delay_min=10000
#simulate_tx_delay_max=50000
#num_user=2
#for num_node in 20
#do 
  #for simulate_tx_delay in 0
  #do
    #for lock_method in 0 1 # 0 = proxy, 1 = direct
    #do
      #if [ $lock_method -eq 0 ]
      #then
        #lock_method_str="proxy"
      #elif [ $lock_method -eq 1 ]
      #then
        #lock_method_str="direct"
      #fi
      #for num_lock_objects in 16 512
      #do
        #for num_requests in 5 10 20 40
        #do
          #for local_workload_ratio in 0 0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1
          #do
            #for shared_lock_ratio in 0 0.25 0.5 0.75 1
            #do
              #bsub -n $num_node -a openmpi -W 2 -m "cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn042 cn043" -R "span[ptile=1]" -R rusage[mem=1024] -J local -eo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-mixed-nn$num_node-lo$num_lock_objects-rq$num_requests-sr$shared_lock_ratio-local$local_workload_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.e -oo /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/rdma_dist_lock/$lock_method_str-mixed-nn$num_node-lo$num_lock_objects-rq$num_requests-sr$shared_lock_ratio-local$local_workload_ratio-simultx$simulate_tx_delay-$simulate_tx_delay_min-$simulate_tx_delay_max.o -q normal "sh rdma_dist_lock.lsf $num_lock_objects $num_requests $num_user $lock_method $workload_type $local_workload_ratio $shared_lock_ratio $duration $simulate_tx_delay $simulate_tx_delay_min $simulate_tx_delay_max"
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
