#!/bin/bash
bsub < ./test_server.lsf
bsub -w "started(rdma_test_server)" < ./test_client.lsf
bsub -w "ended(rdma_test_client)" < ./terminate_server.lsf
