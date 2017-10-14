package carbyne.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Task;
import carbyne.simulator.Simulator;
import carbyne.datastructures.Resources;

public class Machine {

  int machineId;

  boolean execMode;
  public Cluster cluster;

  public double currentTime = 0;

  // max capacity of this machine
  // default is 1.0 across all dimensions
  Resources maxResAlloc;
  Resources totalResAlloc;
  // map: expected completion time -> Task context
  public Map<Task, Double> runningTasks;

  public Machine(int machineId, Resources size, boolean execMode) {
    // LOG.info("Initialize machine: "+machineId+" size:"+size);
    this.machineId = machineId;
    this.execMode = execMode;
    this.currentTime = Simulator.CURRENT_TIME;
    totalResAlloc = new Resources();
    assert size != null;
    maxResAlloc = Resources.clone(size);
    runningTasks = new HashMap<Task, Double>();
  }

  public double earliestFinishTime() {
    double earliestFinishTime = Double.MAX_VALUE;
    for (Double finishTime : runningTasks.values()) {
      earliestFinishTime = Math.min(earliestFinishTime, finishTime);
    }
    return earliestFinishTime;
  }

  public double earliestStartTime() {
    double earliestStartTime = Double.MAX_VALUE;
    for (Double startTime : runningTasks.values()) {
      earliestStartTime = Math.min(earliestStartTime, startTime);
    }
    return earliestStartTime;
  }

  public void assignTask(int dagId, int taskId, double taskDuration,
      Resources taskResources) {
    // TODO - change 0.0 in case of self editing state thing
    currentTime = execMode ? Simulator.CURRENT_TIME : currentTime;

    // if task does not fit -> reject it
    boolean fit = getTotalResAvail().greaterOrEqual(taskResources);
    if (!fit)
      return;

    // 1. update the amount of resources allocated
    totalResAlloc.sum(taskResources);

    // 2. compute the expected time for this task
    double expTaskComplTime = currentTime + taskDuration;
    Task t = new Task(dagId, taskId, taskDuration, taskResources);
    runningTasks.put(t, expTaskComplTime);

    // update resource allocated to the corresponding job
    BaseDag dag = Simulator.getDag(dagId);
    dag.rsrcInUse.sum(dag.rsrcDemands(taskId));
  }

  // [dagId -> List<TaskId>]
  public Map<Integer, List<Integer>> finishTasks(double... finishTime) {

    currentTime = execMode ? Simulator.CURRENT_TIME : (Double) finishTime[0];

    Map<Integer, List<Integer>> tasksFinished = new HashMap<Integer, List<Integer>>();

    Iterator<Map.Entry<Task, Double>> iter = runningTasks.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Task, Double> entry = iter.next();

      if (entry.getValue() <= currentTime) {
        Task t = entry.getKey();
        totalResAlloc.subtract(t.resDemands);

        // update resource freed from corresponding job
        BaseDag dag = Simulator.getDag(t.dagId);
        dag.rsrcInUse.subtract(t.resDemands);

        if (tasksFinished.get(t.dagId) == null) {
          tasksFinished.put(t.dagId, new ArrayList<Integer>());
        }
        tasksFinished.get(t.dagId).add(t.taskId);
        iter.remove();
      }
    }
    return tasksFinished;
  }

  public Resources getTotalResAvail() {
    return Resources.subtract(maxResAlloc, totalResAlloc);
  }

  public int getMachineId() {
    return this.machineId;
  }
}
