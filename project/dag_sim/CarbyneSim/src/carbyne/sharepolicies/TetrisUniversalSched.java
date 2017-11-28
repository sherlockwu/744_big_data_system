package carbyne.sharepolicies;

import carbyne.cluster.Cluster;
import carbyne.cluster.Machine;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Resources;
import carbyne.datastructures.Task;
import carbyne.simulator.Simulator;
import carbyne.utils.Pair;

import java.util.ArrayList;

public class TetrisUniversalSched extends SharePolicy {

  Resources clusterTotCapacity = null;
  ArrayList<Task> runnableTasks = null;

  boolean inclDurInCosineSim = false;

  public TetrisUniversalSched(String policyName, Cluster cluster) {
    super(policyName);
    clusterTotCapacity = cluster.getClusterMaxResAlloc();
  }

  @Override
  public void packTasks(Cluster cluster) {

    runnableTasks = new ArrayList<Task>();

    // among every runnable DAG in the cluster, compute runnable tasks 
    for (BaseDag dag : Simulator.runningJobs) {
      for (int taskId : dag.runnableTasks) {
        runnableTasks.add(new Task(dag.dagId, taskId,
                          dag.duration(taskId), dag.rsrcDemands(taskId)));
      }
    }

    if (runnableTasks.isEmpty()) {
      return;
    }

    Pair<Task, Integer> bestTaskToPack;
    //System.out.println("RUNNABLE TASKS SIZE:"+runnableTasks.size());

    // RG: this can be very expensive - TODO
    // among the runnable tasks, compute the best packing score
    // for each of them w.r.t to every machine they fit.
    while ((bestTaskToPack = computeBestTaskToPack(cluster)).first() != null) {
      //System.out.println("runnable_size:"+runnableTasks.size());
      Task taskToPack = bestTaskToPack.first();
      int taskToPackId = taskToPack.taskId;
      int dagToPackId = taskToPack.dagId;
      int machineTaskToPack = bestTaskToPack.second();

      // try to assign the next task on machineTaskTopack
      boolean assigned = cluster.assignTask(machineTaskToPack, dagToPackId,
          taskToPackId, taskToPack.taskDuration, taskToPack.resDemands);
      //System.out.println("assigned:"+assigned);
      if (assigned) {
        // remove the task from runnable and put it in running
        for (BaseDag dag : Simulator.runningJobs) {
          if (dag.dagId == dagToPackId) {
            dag.runningTasks.add(taskToPackId);
            dag.runnableTasks.remove((Integer) taskToPackId);
          }
        }
        
        runnableTasks.remove(taskToPack);
      }
    }
  }
  
  public Pair<Task, Integer> computeBestTaskToPack(Cluster cluster) {
    Pair<Task, Integer> bestTaskToPack = Pair.createPair(null, -1);

    if (runnableTasks.isEmpty()) {
      return bestTaskToPack;
    }

    double maxScore = Double.MIN_VALUE;
    for (Task task : runnableTasks) {

      double maxScoreTask = Double.MIN_VALUE;
      int maxScoreTaskMachineId = -1;
      Resources taskRes = task.resDemands;
      double taskDur = task.taskDuration;

      for (Machine machine : cluster.getMachines()) {
        Resources machineRes = machine.getTotalResAvail();
        double scoreTaskMachine = Resources.dotProduct(taskRes, machineRes);
        scoreTaskMachine = inclDurInCosineSim ? taskDur + scoreTaskMachine
            : scoreTaskMachine;
        if (maxScoreTask < scoreTaskMachine) {
          maxScoreTask = scoreTaskMachine;
          maxScoreTaskMachineId = machine.getMachineId();
        }
      }

      if (maxScore < maxScoreTask) {
        maxScore = maxScoreTask;
        bestTaskToPack.setFirst(task);
        bestTaskToPack.setSecond(maxScoreTaskMachineId);
      }
    }
    return bestTaskToPack;

  }
}
