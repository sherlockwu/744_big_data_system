package carbyne.carbyne;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;
import carbyne.schedpolicies.SchedPolicy;
import carbyne.schedpolicies.TetrisSchedPolicy;
import carbyne.simulator.Simulator;
import carbyne.utils.Utils;

// per dag
// keeps information for runs at different shares
public class DagExecution {

  public StageDag dag;

  // Schedule policy to schedule inside a dag
  public SchedPolicy schedPolicy;

  // the pool of resources available
  public Cluster cluster;

  // [taskId -> startTime]
  public Map<Double, Set<Integer>> timelineExec;

  public double complTime;

  public DagExecution(StageDag _dag, Resources leftOverResources) {
    dag = StageDag.clone(_dag);
    if (leftOverResources == null) {
      dag.rsrcQuota.subtract(dag.currResDemand());
    } else {
      dag.rsrcQuota = leftOverResources;
    }
    cluster = new Cluster(false, dag.rsrcQuota);
    schedPolicy = new TetrisSchedPolicy(cluster);
    timelineExec = new TreeMap<Double, Set<Integer>>();
    complTime = -1;
  }

  public void schedule(boolean reverse) {
    double currentTime = Simulator.CURRENT_TIME;

    // add to runnable tasks the leaf nodes
    if (reverse) {
      dag.reverseDag();
    }

    // do a full schedule of the dag
    boolean noNewTaskToSched = false, noTaskToFinish = false;
    int sizeDag = dag.allTasks().size();
    while (dag.finishedTasks.size() != sizeDag) {
      noNewTaskToSched = false;
      noTaskToFinish = false;

      // for every runnable tasks try to schedule them
      Set<Integer> oldScheduledTasks = new HashSet<Integer>(dag.runnableTasks);
      schedPolicy.schedule(dag);
      Set<Integer> newScheduledTasks = new HashSet<Integer>(dag.runnableTasks);
      oldScheduledTasks.removeAll(newScheduledTasks);

      if (oldScheduledTasks.isEmpty()) {
        noNewTaskToSched = true;
      } else {
        for (Integer task : oldScheduledTasks) {
          if (timelineExec.get(currentTime) == null)
            timelineExec.put(currentTime, new HashSet<Integer>());
          timelineExec.get(currentTime).add(task);
        }
      }

      // now you need to finish a task to do progress
      // find the earliest finishing time for a task to increment the current
      // time on that machine
      double earliestFinishTime = cluster.earliestFinishTime();
      Map<Integer, List<Integer>> finishedTasks = cluster
          .finishTasks(earliestFinishTime);

      // should always be the case
      if (finishedTasks.get(dag.dagId) != null) {
        ((StageDag) dag).finishTasks(finishedTasks.get(dag.dagId), reverse);
      } else {
        noTaskToFinish = true;
      }
      currentTime = earliestFinishTime;

      // stop condition is when not enough resources to schedule some task
      // so we should pause the timeline execution at this point
      if (noNewTaskToSched && noTaskToFinish) {
        break;
      }
    }
    if (dag.finishedTasks.size() == dag.allTasks().size()) {
      complTime = currentTime;
    }

    // System.out.println("[DagExecution]: " + "dag:" + dag.dagId +
    // " complTime:" + complTime + " timelineExec:" + timelineExec);

    if (!reverse)
      return;

    // in case of reverse schedule we need to reverse the timeline of execution
    Map<Double, Set<Integer>> timelineExecRev = new TreeMap<Double, Set<Integer>>();

    for (Map.Entry<Double, Set<Integer>> entry : timelineExec.entrySet()) {
      double cTime = entry.getKey();
      for (Integer taskId : entry.getValue()) {
        // compl.time rem. - whenStarttask + task_dur + currTime
        double taskStartTime = Utils.round(
            complTime - cTime - dag.duration(taskId) + Simulator.CURRENT_TIME,
            2);
        if (timelineExecRev.get(taskStartTime) == null)
          timelineExecRev.put(taskStartTime, new HashSet<Integer>());
        timelineExecRev.get(taskStartTime).add(taskId);
      }
    }
    timelineExec = new TreeMap<Double, Set<Integer>>(timelineExecRev);
  }
}
