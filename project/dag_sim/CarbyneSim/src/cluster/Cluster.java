package carbyne.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import carbyne.datastructures.Resources;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Simulator;

/**
 * describes the cluster characteristics both per cluster and per machine
 * properties
 */
public class Cluster {

  boolean execMode;
  public Map<Integer, Machine> machines;

  private static Logger LOG = Logger.getLogger(Cluster.class.getName());

  public Cluster(boolean state, Resources res) {
    execMode = state;
    machines = new TreeMap<Integer, Machine>();
    int numberMachines = execMode ? Globals.NUM_MACHINES : 1;
    for (int i = 0; i < numberMachines; ++i) {
      machines.put(i, new Machine(i, res, execMode));
    }
  }

  public boolean assignTask(int machineId, int dagId, int taskId,
      double taskDuration, Resources taskResources) {
    LOG.fine("assign task: "+taskId+" from dag:"+dagId+" on machine:"+machineId);
    Machine machine = machines.get(machineId);
    assert (machine != null);
    boolean fit = machine.getTotalResAvail().greaterOrEqual(taskResources);
    if (!fit) {
      LOG.warning("ERROR; task should fit");
      return false;
    }
    machine.assignTask(dagId, taskId, taskDuration, taskResources);
    return true;
  }

  // checks for fitting in resShare should already be done
  public boolean assignTask(int dagId, int taskId, double taskDuration,
      Resources taskResources) {

    // find the first machine where the task can fit
    // put it there
    for (Machine machine : machines.values()) {
      boolean fit = machine.getTotalResAvail().greaterOrEqual(taskResources);
      if (!fit)
        continue;

      machine.assignTask(dagId, taskId, taskDuration, taskResources);

      return true;
    }
    return false;
  }

  // return: [Key: dagId -- Value: List<taskId>]
  public Map<Integer, List<Integer>> finishTasks(double... earliestFinishTime) {

    // finish any task on this machine at the current time
    Map<Integer, List<Integer>> finishedTasks = new HashMap<Integer, List<Integer>>();

    System.out.println("Cluster starts collecting finihsTasks. Current Time: " + Simulator.CURRENT_TIME);
    for (Machine machine : machines.values()) {
      Map<Integer, List<Integer>> finishedTasksMachine = execMode ? machine
          .finishTasks() : machine.finishTasks((double) earliestFinishTime[0]);

      for (Map.Entry<Integer, List<Integer>> entry : finishedTasksMachine
          .entrySet()) {
        int dagId = entry.getKey();
        List<Integer> tasksFinishedDagId = entry.getValue();
        if (finishedTasks.get(dagId) == null) {
          finishedTasks.put(dagId, new ArrayList<Integer>());
        }
        finishedTasks.get(dagId).addAll(tasksFinishedDagId);
      }
      machine.currentTime = execMode ? 0.0 : (double) earliestFinishTime[0];
    }

    // update the currentTime with the earliestFinishTime on every machine
    return finishedTasks;
  }

  // util classes //
  public Machine getMachine(int machine_id) {
    return machines.get(machine_id);
  }

  public Collection<Machine> getMachines() {
    return machines.values();
  }

  public Resources getClusterMaxResAlloc() {
    Resources maxClusterResAvail = new Resources();
    for (Machine machine : machines.values()) {
      maxClusterResAvail.sum(machine.maxResAlloc);
    }
    return maxClusterResAvail;
  }

  public Resources getClusterResAvail() {
    Resources clusterResAvail = new Resources();
    for (Machine machine : machines.values()) {
      clusterResAvail.sum(machine.getTotalResAvail());
    }
    return clusterResAvail;
  }

  public double earliestFinishTime() {
    double earliestFinishTime = Double.MAX_VALUE;
    for (Machine machine : machines.values()) {
      earliestFinishTime = Math.min(earliestFinishTime,
          machine.earliestFinishTime());
    }
    return earliestFinishTime;
  }

  public double earliestStartTime() {
    double earliestStartTime = Double.MAX_VALUE;
    for (Machine machine : machines.values()) {
      earliestStartTime = Math.min(earliestStartTime,
          machine.earliestStartTime());
    }
    return earliestStartTime;
  }

  // end util classes //
  public boolean containsIntermediateResult(int taskId) {
    for (Machine machine: this.machines.values()) {
      if (machine.containsIntermediateResult(taskId)) {
        return true;
      }
    }
    return false;
  }
}
