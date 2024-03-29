package carbyne.schedulers;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;
import carbyne.schedpolicies.*;
import carbyne.simulator.Main.Globals;

// responsible for scheduling tasks inside a Job
// it can adopt various strategies such as Random, Critical Path
// BFS, Tetris, Graphene, etc.

// return unnecessary resources to the resource pool
public class IntraJobScheduler {

  public SchedPolicy resSchedPolicy;

  public IntraJobScheduler(Cluster cluster) {

    switch (Globals.INTRA_JOB_POLICY) {
    case Random:
      resSchedPolicy = new RandomSchedPolicy(cluster);
      break;
    case BFS:
      resSchedPolicy = new BFSSchedPolicy(cluster);
      break;
    case CP:
      resSchedPolicy = new CPSchedPolicy(cluster);
      break;
    case Tetris:
      resSchedPolicy = new TetrisSchedPolicy(cluster);
      break;

    default:
      System.err.println("Unknown sharing policy");
    }
  }

  public void schedule(StageDag dag) {
    // while tasks can be assigned in my resource
    // share quanta, on any machine, keep assigning
    // otherwise return
    resSchedPolicy.schedule(dag);
  }

  public double planSchedule(StageDag dag, Resources leftOverResources) {
    return resSchedPolicy.planSchedule(dag, leftOverResources);
  }
}
