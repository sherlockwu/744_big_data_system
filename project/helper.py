import random
import json
import csv
import os
import subprocess

config = json.load(open('dag_sim/CarbyneSim/inputs/config_test.json'))
dags = json.load(open('dag_sim/CarbyneSim/inputs/dags-input_test.json'))

# For parse cluster and DAG Information
def parseCluster():   #TODO
    machineKey = 'machines'
    replicaKey = 'replica'
    numMachine = config[machineKey][0][replicaKey]

    return numMachine

def parseIntermediate():
    intermediateOutputStoreOnMachinesNumber = []
    os.chdir('dag_sim/CarbyneSim')
    res = subprocess.call("./run_demo_first.sh", shell=True)
    os.chdir('../..')
    intermediate = open("dag_sim/CarbyneSim/inputs/stage_data_to_machine_distribution")
    for row in intermediate:
        arr = row.split(',')
        if len(arr) != 3:
            continue
        intermediateOutputStoreOnMachinesNumber.append(int(arr[2]))
    intermediate.close()
    return intermediateOutputStoreOnMachinesNumber


class Node():
    run_time_ = 0
    parent_ = -1

    def __init__(self, run_time, parent):
        self.run_time_ = run_time
        self.parent_ = parent

def parseDAG():        #TODO
    # DAG = [Node(4.0, -1), Node(6.0, 0), Node(10.0, 1)]
    # Ns = 3
    # n = [3, 4, 1]

    # return DAG, Ns, n
    singleDag = dags[0]
    stageKey = "stages"
    stageRuntimeKey = "stageRuntime"
    dependencyKey = "dependencies"
    durationKey = 'duration'

    DAG = {}

    intermediateOutputStoreOnMachinesNumber = parseIntermediate()
    stageRunTime = []
    stages = singleDag[stageKey]
    numStage = len(stages)
    dependency = singleDag[dependencyKey]
    parent = {}
    for stage in stages:
        stageRunTime.append(stage[durationKey])

    for ele in dependency:
        src = ele['src']
        dst = ele['dst']
        parent[dst] = src

    DAG[stageRuntimeKey] = stageRunTime
    DAG[dependencyKey] = parent

    DAG_RESULT = []
    for i in range(len(stageRunTime)):
        run_time = stageRunTime[i]
        if i > 0:
            cur_parent = int(parent["Stage_"+str(i)].split('_')[-1])
            print "=== this parent ", cur_parent
        else:
            cur_parent = -1
        DAG_RESULT.append(Node(run_time, cur_parent))

    return DAG_RESULT, numStage, intermediateOutputStoreOnMachinesNumber


# For optimization

## Randomly generate placement for one task
def random_generate_one_task( Nm, ni ):
    return random.sample(range(Nm), ni)

## Random generate placement for whole DAG
def random_generate_one(Nm, Ns, n):
    res = []
    for i in range(Ns-1):
        print i, len(n), Ns
        res.append( random_generate_one_task(Nm, n[i]) )

    return res # a dataplacement

## calculate score of this placment strategy
def calculate_score(placement, DAG):
    print "Calculating score for ", placement
    fail_prob = 0.01 # as parameter?

    # running time
    run_time = get_time_from_simulator(placement)
    print "                       Running time ", run_time

    # failure recovery time
    failure_recovery_time = calculate_failure_recovery_time(run_time, placement, DAG)
    print "                       Expected failure recovery time ", failure_recovery_time

    score = run_time + fail_prob * failure_recovery_time
    print "                       Final Score ", score

    return score # score of this placement


def get_time_from_simulator(placement):   #TODO
    with open("dag_sim/CarbyneSim/inputs/deploy.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(placement)
    os.chdir('dag_sim/CarbyneSim')
    res = subprocess.check_output("./run_demo_not_first.sh", shell=True)
    os.chdir('../..')
    arr = res.split('\n')
    return float(arr[-3].split()[1])


def recover(machine, task, DAG, placement):
    # recover until parent node doesn't store output on this machine
    recover_time = 0
    cur_task = task
    print task
    while (machine in set(placement[cur_task])):
        recover_time += DAG[cur_task].run_time_
        cur_task = DAG[cur_task].parent_
        cur_task
        if cur_task == -1:
            break
    return recover_time


def calculate_failure_recovery_time(run_time, placement, DAG):
    print "== calculating for ", placement
    # caculate failure recovery time
    overall_recovery_time = 0
    # dump DAG
    for i in range(len(DAG)-1):
        node = DAG[i]
        #print "This node: ", node.run_time_, " parent: ", node.parent_
        # calculate probability of occuring a failure during this period of time of task
        task_fail_prob = (float(node.run_time_)) / run_time
        #print task_fail_prob
        task_recovery_time = 0
        for machine in placement[i]:
            # calculate failure probability
            machine_fail_prob = float(1)/len(placement[i])
            #print machine_fail_prob
            machine_recovery_time = recover(machine, i, DAG, placement)
            task_recovery_time += machine_recovery_time * machine_fail_prob
        overall_recovery_time += task_recovery_time * task_fail_prob
    #print overall_recovery_time
    return overall_recovery_time
