package carbyne.F2;

import carbyne.cluster.Cluster;
import carbyne.cluster.Machine;
import carbyne.datastructures.*;
import carbyne.schedulers.InterJobScheduler;
import carbyne.schedulers.IntraJobScheduler;

import javax.crypto.Mac;
import java.util.*;
import java.util.logging.Logger;

/*
  TODO：
  1 when to check finished tasks? each ready receiving?
  2 need to test whether tasks are really added to the dag. see StageDag::addRunnableTask(Task, int, int, String)
 */
public class ExecuteService {

  private static Logger LOG = Logger.getLogger(StageDag.class.getName());
  private Cluster cluster_;
  private InterJobScheduler interJobScheduler_;
  private IntraJobScheduler intraJobScheduler_;
  private Queue<BaseDag> runningJobs_;
  private int nextId_;

  public Map<Task, Double> runningTasks_;

  public ExecuteService(Cluster cluster, InterJobScheduler interJobScheduler,
                        IntraJobScheduler intraJobScheduler,
                        Queue<BaseDag> runningJobs) {
    cluster_ = cluster;
    interJobScheduler_ = interJobScheduler;
    intraJobScheduler_ = intraJobScheduler;
    runningJobs_ = runningJobs;
    nextId_ = 0;
  }

  public void receiveReadyEvents(boolean needInterJobScheduling, Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    Map<Integer, Set<String>> dagRunnableStagesMap = new HashMap<>();
    Set<String> newRunnableStageNameSet = null;
    int dagId = -1;
    if(needInterJobScheduling) {
      interJobScheduler_.schedule(cluster_);
    }
    LOG.info("Running jobs size:" + runningJobs_.size());
    for (BaseDag dag: runningJobs_) {
      StageDag rDag = (StageDag)dag;
      dagRunnableStagesMap.put(rDag.getDagId(),
          rDag.setInitiallyRunnableStages());
    }
    ReadyEvent readyEvent = readyEventQueue.poll();
    while(readyEvent != null) {
      LOG.info("Receive a ready event: " + readyEvent.toString());
      dagId = readyEvent.getDagId();
      if (!dagRunnableStagesMap.containsKey(dagId)) {
        dagRunnableStagesMap.put(dagId, new HashSet<>());
      }
      receiveReadyEvent(readyEvent, dagRunnableStagesMap.get(dagId));
      readyEvent = readyEventQueue.poll();
    }

    for (Map.Entry<Integer, Set<String>> entry: dagRunnableStagesMap.entrySet()) {
      dagId = entry.getKey();
      BaseDag dag = getDagById(dagId);
      newRunnableStageNameSet = entry.getValue();
      for(String newRunnableStageName : newRunnableStageNameSet) {
        int taskId = nextId_;
        nextId_++;
        //assume the stage duration and demands are successfully loaded at the beginning of simulator
        Stage newRunnableStage = ((StageDag)dag).stages.get(newRunnableStageName);

        double newTaskDuration = newRunnableStage.vDuration;
        Resources newTaskRsrcDemands = new Resources(newRunnableStage.vDemands);
        Task task = new Task(dagId, taskId, newTaskDuration, newTaskRsrcDemands);
        ((StageDag)dag).addRunnableTask(task, taskId, newRunnableStage.id, newRunnableStageName);
      }
      schedule(dagId);
    }

    emitSpillEvents(spillEventQueue);
  }

  private void receiveReadyEvent(ReadyEvent readyEvent, Set<String> newRunnableStageNameSet) {
    int dagId = readyEvent.getDagId();
    BaseDag dag = getDagById(dagId);
    //fetch data from the partition of this readyEvent
    int stageId = readyEvent.getStageId();
    String stageName = readyEvent.getStageName();
    Partition partition = readyEvent.getPartition();
    if(!partition.isLastPartReady()) { return; }

    List<Integer> machines = partition.getMachinesInvolved();
    double max = -1;
    int id = -1;
    for(Integer machineId : machines) {
      double cur = partition.getPartitionSizeOnMachine(machineId);
      if(max < cur) {
        max = cur;
        id = machineId;
      }
    }

    //copy data to a single node if more than 1 machine is data holder
    if(machines.size() > 1) {
      partition.aggregateKeyShareToSingleMachine(id, machines);
    }
    newRunnableStageNameSet.addAll(updateRunnable(stageName, dag));
  }

  Set<String> updateRunnable(String stageName, BaseDag dag) {
    StageDag stageDag = (StageDag) dag;
    //just to make sure, ancestors is a pointer, not a new map, otherwise line 114 should be modified.
    /* Map<String, Set<String>> ancestors = stageDag.ancestorsS;
    Map<String, Set<String>> children = stageDag.descendantsS; */
    stageDag.moveRunningToFinish(stageName);
    Set<String> result = stageDag.updateRunnableStages();
    /* if(children.get(stageName) == null) {
      System.out.println("stage id does not exist, cannot update ancestors");
      return result;
    }
    Set<String> candidates = children.get(stageName);
    for(String candidate: candidates) {
      Set<String> currentAncestors = ancestors.get(candidate);

      if (!currentAncestors.contains(stageName)) {
        System.out.println("Wrong topology, ancestors and children map conflict");
        return result;
      }
      currentAncestors.remove(stageName);

      if (currentAncestors.isEmpty()) {
        System.out.println("now runnable:" + candidate);
      }

      ancestors.put(candidate, currentAncestors);
    } */
    return result;
  }

  private void schedule(int dagId) {
    BaseDag dag = getDagById(dagId);
    if(dag == null) {
      System.out.println("Error: Dag is not running any more when trying to schedule");
      return;
    }
    intraJobScheduler_.schedule((StageDag) dag);
  }

  private BaseDag getDagById(int dagId) {
    BaseDag dag = null;
    for(BaseDag e : runningJobs_) {
      if(e.getDagId() == dagId) {
        dag = e;
        break;
      }
    }
    return dag;
  }

  private void emitSpillEvents(Queue<SpillEvent> spillEventQueue) {
    //check finish tasks machine by machine
    //when to finish? only on receive ready??
    List<Machine> machines = this.cluster_.getMachinesList();
    //iterate machine by machine
    for(Machine machine : machines) {
      Map<Integer, List<Integer>> finishedTasksPerMachine = machine.finishTasks();
      //iterate dag by dag
      for(Map.Entry<Integer, List<Integer>> entry : finishedTasksPerMachine.entrySet()) {
        int dagId = entry.getKey();
        List<Integer> tasksFinished = entry.getValue();
        emit(spillEventQueue, dagId, tasksFinished);
      }
    }
  }

  private void emit(Queue<SpillEvent> spillEventQueue, int dagId, List<Integer> taskFinished) {
    for(Integer taskId : taskFinished) {
      Map<Integer, Double> data = new HashMap<>();
      boolean lastSpill = false;
      int stageId = ((StageDag)getDagById(dagId)).getStageIdByTaskId(taskId);
      String stageName = ((StageDag)getDagById(dagId)).vertexToStage.get(taskId);
      double timestamp = 0;
      // TODO: add stage name
      SpillEvent spill = new SpillEvent(data, lastSpill, dagId, stageId, stageName, taskId, timestamp);
      spillEventQueue.add(spill);
    }
  }

  private int getRandom() {
    Random rand = new Random();
    return rand.nextInt(1000);
  }
}
