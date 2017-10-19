package carbyne.schedulers;

import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;
import carbyne.schedpolicies.BFSSchedPolicy;
import carbyne.schedpolicies.CPSchedPolicy;
import carbyne.schedpolicies.CarbyneSchedPolicy;
import carbyne.schedpolicies.RandomSchedPolicy;
import carbyne.schedpolicies.SchedPolicy;
import carbyne.schedpolicies.TetrisSchedPolicy;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Simulator;

// responsible for scheduling tasks inside a Job
// it can adopt various strategies such as Random, Critical Path
// BFS, Tetris, Graphene, etc.

// return unnecessary resources to the resource pool
public class IntraJobScheduler {

  public SchedPolicy resSchedPolicy;

  public IntraJobScheduler() {

    switch (Globals.INTRA_JOB_POLICY) {
    case Random:
      resSchedPolicy = new RandomSchedPolicy(Simulator.cluster);
      break;
    case BFS:
      resSchedPolicy = new BFSSchedPolicy(Simulator.cluster);
      break;
    case CP:
      resSchedPolicy = new CPSchedPolicy(Simulator.cluster);
      break;
    case Tetris:
      resSchedPolicy = new TetrisSchedPolicy(Simulator.cluster);
      break;
    case Carbyne:
      resSchedPolicy = new CarbyneSchedPolicy(Simulator.cluster);
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
