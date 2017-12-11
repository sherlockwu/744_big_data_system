import helper
import EA

def strategy_generator( Nm, Ns, n, DAG ):
    # todo, change different algorithms
    return EA.EA_Handler( Nm, Ns, n, DAG)


if __name__ == '__main__':

    # Get Cluster Configuration
    Nm = helper.parseCluster()
    print "=== this is a cluster with ", Nm, "machines"

    # Get DAG information
    DAG, Ns, n = helper.parseDAG()
    print "=== DAG parsed having ", Ns, "task", " each:", n
    print len(DAG)
    for i in range(len(DAG)):
        print "This node: ", DAG[i].run_time_, " parent: ", DAG[i].parent_


    # DAG : need 1. time 2. dependency
    

    # Configuration :  (Nm, Ns)   Nm:number of machines,   Ns: number of taskes in this DAG
    #               :  n: task i's output are assigned to ni machines
    # Solution Space:
    #                  ((M11,M12...M1n1), ... (Mi1, Mi2, ... Mini), .... (MNs1, ... MNsnNs))    -> score
    #                  Mij != Mik
    #                  Mij in (0..Nm-1)

    best_placement, best_score = strategy_generator( Nm, Ns, n, DAG )

    print best_placement, 0-best_score
    run_time = helper.get_time_from_simulator(best_placement)
    failure_recovery_time = helper.calculate_failure_recovery_time(run_time, best_placement, DAG)
    print "Running Time          : ", run_time 
    print "failure_recovery_time : ", failure_recovery_time




    print "====== For Round-Robin :"
    #run_time = helper.get_time_from_simulator(generate_round_robin()) #TODO
    #failure_recovery_time = helper.calculate_failure_recovery_time(run_time, round_robin_placement, DAG)
