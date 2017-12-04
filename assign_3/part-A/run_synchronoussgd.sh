#!/bin/bash

# tfdefs.sh has helper function to start process on all VMs
# it contains definition for start_cluster and terminate_cluster
source tfdefs.sh

terminate_cluster
# startserver.py has the specifications for the cluster.
start_cluster startserver.py

# testdistributed.py is a client that can run jobs on the cluster.
# please read testdistributed.py to understand the steps defining a Graph and
# launch a session to run the Graph
python synchronoussgd.py

# defined in tfdefs.sh to terminate the cluster
terminate_cluster
