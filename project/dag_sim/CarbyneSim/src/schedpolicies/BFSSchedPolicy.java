package carbyne.schedpolicies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;

public class BFSSchedPolicy extends SchedPolicy {

  public BFSSchedPolicy(Cluster cluster) {
    super(cluster);
  }

  @Override
  public void schedule(final StageDag dag) {

    if (dag.runnableTasks.isEmpty())
      return;

    ArrayList<Integer> rtCopy = new ArrayList<Integer>(dag.runnableTasks);

    // among the runnable tasks:
    // pick one which has the largest CP
    // as the next task to schedule
    Collections.sort(rtCopy, new Comparator<Integer>() {
      public int compare(Integer arg0, Integer arg1) {
        Double val0 = dag.BFSOrder.get(arg0);
        Double val1 = dag.BFSOrder.get(arg1);
        return (int) (val1 - val0);
      }
    });
    Iterator<Integer> iter = rtCopy.iterator();
    while (iter.hasNext()) {
      int taskId = iter.next();

      // discard tasks whose resource requirements are larger than total share
      boolean fit = dag.currResShareAvailable().greaterOrEqual(
          dag.rsrcDemands(taskId));
      if (!fit)
        continue;

      // try to assign the next task on a machine
      boolean assigned = cluster.assignTask(dag.dagId, taskId,
          dag.duration(taskId), dag.rsrcDemands(taskId));

      if (assigned) {
        // remove the task from runnable and put it in running
        dag.runningTasks.add(taskId);
        iter.remove();
        dag.runnableTasks.remove(taskId);
      }
    }
  }

  @Override
  public double planSchedule(StageDag dag, Resources leftOverResources) {
    return -1;
  }
}
