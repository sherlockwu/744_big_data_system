import random

# For parse cluster and DAG Information
def parseCluster():   #TODO
    return 10 

class Node():
    run_time_ = 0
    parent_ = -1

    def __init__(self, run_time, parent):
        self.run_time_ = run_time
        self.parent_ = parent

def parseDAG():        #TODO
    DAG = [Node(4.0, -1), Node(6.0, 0), Node(10.0, 1)]
    Ns = 3
    n = [3, 4, 1]
 
    return DAG, Ns, n



# For optimization

## Randomly generate placement for one task
def random_generate_one_task( Nm, ni ):
    return random.sample(range(Nm), ni)

## Random generate placement for whole DAG
def random_generate_one(Nm, Ns, n):
    res = []
    for i in range(Ns):
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
    return 10.0

def recover(machine, task, DAG, placement):
    # recover until parent node doesn't store output on this machine
    recover_time = 0
    cur_task = task
    while (machine in set(placement[cur_task])):
        recover_time += DAG[cur_task].run_time_
        cur_task = DAG[cur_task].parent_
        if cur_task == -1:
            break
    return recover_time


def calculate_failure_recovery_time(run_time, placement, DAG):
    # caculate failure recovery time
    overall_recovery_time = 0
    # dump DAG
    for i in range(len(DAG)):
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
