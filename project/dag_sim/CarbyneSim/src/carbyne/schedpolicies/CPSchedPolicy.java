package carbyne.schedpolicies;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import java.util.logging.Logger;
/**
 * CP scheduling class
 * */
public class CPSchedPolicy extends SchedPolicy {

  private static Logger LOG = Logger.getLogger(CPSchedPolicy.class.getName());
  public CPSchedPolicy(Cluster cluster) {
    super(cluster);
  }

  @Override
  public void schedule(final StageDag dag) {

    // no tasks to be scheduled -> skip
    LOG.fine("Dag: " + dag.dagId + " runnable tasks:" + dag.runnableTasks.size());
    if (dag.runnableTasks.isEmpty())
      return;

    ArrayList<Integer> rtCopy = new ArrayList<Integer>(dag.runnableTasks);

    // among the runnable tasks:
    // pick one which has the largest CP
    // as the next task to schedule
    Collections.sort(rtCopy, new Comparator<Integer>() {
      @Override
      public int compare(Integer arg0, Integer arg1) {
        Double val0 = dag.CPlength.get(dag.vertexToStage.get(arg0));
        Double val1 = dag.CPlength.get(dag.vertexToStage.get(arg1));
        if (val0 > val1)
          return -1;
        if (val0 < val1)
          return 1;
        return 0;
      }
    });
    Iterator<Integer> iter = rtCopy.iterator();
    // dag.printCPLength();
    while (iter.hasNext()) {
      int taskId = iter.next();

      // discard tasks whose resource requirements are larger than total share
      boolean fit = dag.currResShareAvailable().greaterOrEqual(
          dag.rsrcDemands(taskId));
      if (!fit) {
        LOG.finest("Task " + taskId + " does not fit quota " + dag.dagId + ". task resource demands: " + dag.rsrcDemands(taskId) + " dag resource available: " + dag.currResShareAvailable());
        continue;
      }

      // try to assign the next task on a machine
      int preferedMachine = dag.getAssignedMachine(taskId);
      boolean assigned = false;
      if (preferedMachine == -1) {
        assigned = cluster_.assignTask(dag.dagId, taskId,
            dag.duration(taskId), dag.rsrcDemands(taskId));
      } else {
        assigned = cluster_.assignTask(preferedMachine, dag.dagId, taskId,
            dag.duration(taskId), dag.rsrcDemands(taskId));
      }

      if (assigned) {
        // remove the task from runnable and put it in running
        dag.runningTasks.add(taskId);
        dag.launchedTasksNow.add(taskId);
        iter.remove();
        dag.runnableTasks.remove(taskId);
        String stage = dag.vertexToStage.get(taskId);
        if (!dag.isRunningStage(stage)) {
          dag.moveRunnableToRunning(stage);
        }
      }
    }

    dag.printLaunchedTasks();
    // clear the list of tasks launched as of now
    dag.launchedTasksNow.clear();
  }

  @Override
  public double planSchedule(StageDag dag, Resources leftOverResources) {
    return -1;
  }
}
