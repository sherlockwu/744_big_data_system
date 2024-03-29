#!/bin/bash

source tfdefs.sh
terminate_cluster
start_cluster startserver.py
# start multiple clients
nohup python batchasyncsgd.py --task_index=1 > asynclog-1.out 2>&1&
sleep 30 # wait for variable to be initialized
nohup python batchasyncsgd.py --task_index=0 > asynclog-0.out 2>&1&
nohup python batchasyncsgd.py --task_index=2 > asynclog-2.out 2>&1&
nohup python batchasyncsgd.py --task_index=3 > asynclog-3.out 2>&1&
nohup python batchasyncsgd.py --task_index=4 > asynclog-4.out 2>&1&
sleep 500
