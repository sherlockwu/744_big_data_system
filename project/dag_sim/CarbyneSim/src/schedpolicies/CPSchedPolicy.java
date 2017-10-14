package carbyne.schedpolicies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;

/**
 * CP scheduling class
 * */
public class CPSchedPolicy extends SchedPolicy {

  public CPSchedPolicy(Cluster cluster) {
    super(cluster);
  }

  @Override
  public void schedule(final StageDag dag) {

    // no tasks to be scheduled -> skip
    if (dag.runnableTasks.isEmpty())
      return;

    ArrayList<Integer> rtCopy = new ArrayList<Integer>(dag.runnableTasks);

    // among the runnable tasks:
    // pick one which has the largest CP
    // as the next task to schedule
    Collections.sort(rtCopy, new Comparator<Integer>() {
      @Override
      public int compare(Integer arg0, Integer arg1) {
        Double val0 = dag.CPlength.get(arg0);
        Double val1 = dag.CPlength.get(arg1);
        if (val0 > val1)
          return -1;
        if (val0 < val1)
          return 1;
        return 0;
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
        // System.out.println("Assigned task:" + taskId);
        // remove the task from runnable and put it in running
        dag.runningTasks.add(taskId);
        dag.launchedTasksNow.add(taskId);
        iter.remove();
        dag.runnableTasks.remove(taskId);
      }
    }

    // clear the list of tasks launched as of now
    dag.launchedTasksNow.clear();
  }

  @Override
  public double planSchedule(StageDag dag, Resources leftOverResources) {
    return -1;
  }
}
