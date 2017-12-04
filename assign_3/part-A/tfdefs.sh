#!/bin/bash
export TF_RUN_DIR="/home/ubuntu/run/"
export TF_BINARY_URL="https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.11.0rc2-cp27-none-linux_x86_64.whl"

# modify the below to your group number
export GROUP_NUM=21

function terminate_cluster() {
    echo "Terminating the servers"
#    CMD="ps aux | grep -v 'grep' | grep 'python startserver' | awk -F' ' '{print $2}' | xargs kill -9"
    CMD="ps aux | grep -v 'grep' | grep -v 'bash' | grep -v 'ssh' | grep 'python startserver' | awk -F' ' '{print \$2}' | xargs kill -9"
    for i in `seq 1 5`; do
        ssh ubuntu@vm-$GROUP_NUM-$i "$CMD"
    done
}


function install_tensorflow() {
    pdsh -R ssh -w vm-$GROUP_NUM-[1-5] "sudo apt-get install --assume-yes python-pip python-dev"
    pdsh -R ssh -w vm-$GROUP_NUM-[1-5] "sudo pip install --upgrade $TF_BINARY_URL"
}


function start_cluster() {
    if [ -z $1 ]; then
        echo "Usage: start_cluster <python script>"
        echo "Here, <python script> contains the cluster spec that assigns an ID to all server."
    else
        echo "Create $TF_RUN_DIR on remote hosts if they do not exist."
        pdsh -R ssh -w vm-$GROUP_NUM-[1-5] "mkdir -p $TF_RUN_DIR"
        echo "Copying the script to all the remote hosts."
        pdcp -R ssh -w vm-$GROUP_NUM-[1-5] $1 $TF_RUN_DIR

        echo "Starting tensorflow servers on all hosts based on the spec in $1"
        echo "The server output is logged to serverlog-i.out, where i = 1, ..., 5 are the VM numbers."
        nohup ssh ubuntu@vm-$GROUP_NUM-1 "cd /home/ubuntu/run ; python startserver.py --task_index=0" > serverlog-1.out 2>&1&
        nohup ssh ubuntu@vm-$GROUP_NUM-2 "cd /home/ubuntu/run ; python startserver.py --task_index=1" > serverlog-2.out 2>&1&
        nohup ssh ubuntu@vm-$GROUP_NUM-3 "cd /home/ubuntu/run ; python startserver.py --task_index=2" > serverlog-3.out 2>&1&
        nohup ssh ubuntu@vm-$GROUP_NUM-4 "cd /home/ubuntu/run ; python startserver.py --task_index=3" > serverlog-4.out 2>&1&
        nohup ssh ubuntu@vm-$GROUP_NUM-5 "cd /home/ubuntu/run ; python startserver.py --task_index=4" > serverlog-5.out 2>&1&
    fi
}

