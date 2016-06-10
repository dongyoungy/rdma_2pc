#!/bin/bash
DURATION=60
DURATION_IN_MIN=$(($DURATION/60+1))

for nn in {1..40}
do
  NUM_NODE=$nn
  for d in 2 4 8 16 32 64 128 256 512 
  #for d in 1 1024
  #for ds in 1073741824
  do
    ds=$(($d*1048576))
    DATA_SIZE=$ds
    DATA_SIZE_MB=$(($DATA_SIZE/1048576+128))
    timestamp=`date +%s`
    bsub -E "ls" -m "cn007" < ./test_server.lsf
    #bsub -K -w "ended(rdma_test_server)&&ended(rdma_test_client)&&ended(rdma_test_terminate)" -m "cn007" < ./test_server.lsf &
    # bsub -m "cn001 cn002 cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -w "started(rdma_test_server)" < ./test_client.lsf 1 $DURATION $DATA_SIZE
    bsub -w "started(rdma_test_server)" -m "cn001 cn002 cn003 cn004 cn005 cn006 cn008 cn009 cn010 cn011 cn012 cn013 cn014 cn015 cn016 cn017 cn018 cn019 cn020 cn021 cn022 cn023 cn024 cn025 cn026 cn027 cn028 cn029 cn030 cn031 cn032 cn033 cn034 cn035 cn036 cn037 cn038 cn039 cn040 cn041 cn042 cn043" -a openmpi -W $DURATION_IN_MIN -n $NUM_NODE -R "rusage[mem=$DATA_SIZE_MB]" -J rdma_test_client -e /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/test_client/err/%J.e -o /gpfs/gpfs0/groups/mozafari/dyoon/work/lsf_log/test_client/${DATA_SIZE}_${NUM_NODE}_${timestamp}.out -q normal  mpirun.lsf --mca pml ob1 --mca btl self,sm,openib /gpfs/gpfs0/groups/mozafari/dyoon/work/rdma_2pc/bin/test_client /gpfs/gpfs0/groups/mozafari/dyoon/work/temp 1 $DURATION $DATA_SIZE
    bsub -w "ended(rdma_test_client)" < ./terminate_server.lsf
    num_jobs=`bjobs 2>&1 | wc -l`
    num_jobs=$(($num_jobs-1))
    while [ $num_jobs -gt 0 ]
    do
      sleep 1
      num_jobs=`bjobs 2>&1 | wc -l`
      num_jobs=$((num_jobs-1))
    done
  done
done
