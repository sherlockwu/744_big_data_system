package carbyne.F2;

import carbyne.cluster.Cluster;
import carbyne.cluster.Machine;
import carbyne.datastructures.*;
import carbyne.schedulers.InterJobScheduler;
import carbyne.schedulers.IntraJobScheduler;

import javax.crypto.Mac;
import java.util.*;

public class ExecuteService {

  private Cluster cluster_;
  private InterJobScheduler interJobScheduler_;
  private IntraJobScheduler intraJobScheduler_;
  private Queue<BaseDag> runningJobs_;
  private Map<Integer, Set<Integer>> ancestors;
  private Map<Integer, Integer> children;
  public Map<Task, Double> runningTasks_;

  public ExecuteService(Cluster cluster, InterJobScheduler interJobScheduler,
                        IntraJobScheduler intraJobScheduler,
                        Queue<BaseDag> runningJobs) {
    cluster_ = cluster;
    interJobScheduler_ = interJobScheduler;
    intraJobScheduler_ = intraJobScheduler;
    runningJobs_ = runningJobs;
    ancestors = new HashMap<>();
    children = new HashMap<>();
  }

  public void receiveReadyEvents(boolean needInterJobScheduling, Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    if(needInterJobScheduling) {
      interJobScheduler_.schedule(cluster_);
    }
    ReadyEvent readyEvent = readyEventQueue.poll();
    while(readyEvent != null) {
      receiveReadyEvent(readyEvent);
      readyEvent = readyEventQueue.poll();
    }

    emitSpillEvents(spillEventQueue, 0);
  }

  private void receiveReadyEvent(ReadyEvent readyEvent) {
    //fetch data from the partition of this readyEvent
    int dagId = readyEvent.getDagId();
    BaseDag dag = getDagById(dagId);
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

    if(machines.size() > 1) {
      //copy data to a single node
      partition.aggregateKeyShareToSingleMachine(id, machines);
    }
    int newRunnableStageId = updateAncestors(stageId);
    if(newRunnableStageId == -1) //need to wait for other ancestors to complete
      return;

    int taskId = -1;
    Stage newRunnableStage = ((StageDag)dag).stages.get(stageName);

    double newTaskDuration = newRunnableStage.vDuration;
    Resources newTaskRsrcDemands = new Resources(newRunnableStage.vDemands);
    Task task = new Task(dagId, getRandom(), newTaskDuration, newTaskRsrcDemands);

    //now runnable tasks updated in Simulator::updateJobsStatus, need to modify
    //so that runnable tasks will be updated according to the ready events
    schedule(dagId, taskId);
  }

  int getRandom() {
    Random rand = new Random();
    return rand.nextInt(1000);
  }

  int updateAncestors(int stageId) {
    if(children.get(stageId) == null) {
      System.out.println("stage id does not exist, cannot update ancestors");
      return -1;
    }
    int candidate = children.get(stageId);
    Set<Integer> currentAncestors = this.ancestors.get(candidate);

    if(!currentAncestors.contains(stageId)) {
      System.out.println("Wrong topology, ancestors and children map conflict");
      return -1;
    }
    currentAncestors.remove(stageId);
    if(currentAncestors.isEmpty()) {
      System.out.println("now runnable:" + Integer.toString(candidate));
    }

    this.ancestors.put(candidate, new HashSet<>());
    return candidate;
  }

  private void schedule(int dagId, int taskId) {
    BaseDag dag = getDagById(dagId);
    if(dag == null) {
      System.out.println("Error: Dag is not running any more when trying to schedule");
      return;
    }

    //add taskId to runnable tasks
    //Should also add duration and rsrcDemands? Because creating new tasks, no related info.
    dag.runnableTasks.add(taskId);

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

  private void emitSpillEvents(Queue<SpillEvent> spillEventQueue, double currentTime) {
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
        emit(spillEventQueue, dagId, tasksFinished, currentTime);
      }
    }
  }

  private void emit(Queue<SpillEvent> spillEventQueue, int dagId, List<Integer> taskFinished, double currentTime) {
    for(Integer taskId : taskFinished) {
      Map<Integer, Double> data = new HashMap<>();
      boolean lastSpill = false;
      int stageId = ((StageDag)getDagById(dagId)).getStageIdByTaskId(taskId);
      SpillEvent spill = new SpillEvent(data, lastSpill, dagId, stageId, taskId, currentTime);
      spillEventQueue.add(spill);
    }
  }
}
