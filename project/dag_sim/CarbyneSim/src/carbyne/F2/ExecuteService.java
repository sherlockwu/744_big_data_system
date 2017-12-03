package carbyne.F2;

import carbyne.cluster.Cluster;
import carbyne.cluster.Machine;
import carbyne.datastructures.*;
import carbyne.schedulers.InterJobScheduler;
import carbyne.schedulers.IntraJobScheduler;
import carbyne.simulator.Simulator;

import javax.crypto.Mac;
import java.util.*;
import java.util.logging.Logger;

/*
  TODO：
  1 when to check finished tasks? each ready receiving?
  2 need to test whether tasks are really added to the dag. see StageDag::addRunnableTask(Task, int, int, String)

  e.x.
  DAG0: initial data [50, 50, 50, 50, 50, 50, 50, 50, 50, 50]
  Stage0 --- ata --> Stage1 --- o2o --> Stage2
  machnines: M1, M2
  globalpart: 2

  Stage0
  ES
  task0
    - input: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
    - M1
    - output: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
  task1:
    - input: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
    - M1
    - output: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)

  spill event1: (0, 0, 0), (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
  spill event2: (0, 0, 1), (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25) last
  DS
  Stage0 Partitions (0, 0)
  on spill event1 arrival
M1: Partition[0](:= 0 mod 4) (0, 25), (4, 25), (8, 25)
    Partition[1](:= 1 mod 4) (1, 25), (5, 25), (9, 25)
M2: Partition[2](:= 2 mod 4) (2, 25), (6, 25)
    Partition[2](:= 3 mod 4) (3, 25), (7, 25)
  on spill event2 arrival
M1: Partition[0](:= 0 mod 4) (0, 50), (4, 50), (8, 50)
    Partition[1](:= 1 mod 4) (1, 50), (5, 50), (9, 50)
M2: Partition[2](:= 2 mod 4) (2, 50), (6, 50)
    Partition[2](:= 3 mod 4) (3, 50), (7, 50)


 ready event1 (0, 0) Partition[0] M1
 ready event2 (0, 0) Partition[1] M1
 ready event3 (0, 0) Partition[2] M2
 ready event4 (0, 0) Partition[3] M2 last

  ES
  on ready event1 arrival
  collect (0, 0): incomplete, M1: {P1}
  on ready event2 arrival
  collect (0, 0): incomplete, M1: {P1, P2}
  on ready event3 arrival
  collect (0, 0): incomplete, M1: {P1, P2}, M2: {P3}
  on ready event4 arrival
  collect (0, 0): complete, M1: {P1, P2}, M2: {P3, P4}

  new runnable stages: Stage1
  Stage1
  M1
    task2
      -input: P1, P2
      -output: {(0, 25), (4, 25), (8, 25), (1, 25), (5, 25), (9, 25)}
  M2
    task3
      -input: P3, P4
      -output: {(2, 25), (6, 25), (3, 25), (7, 25)}

  spill event3: (0, 1, 2) {(0, 25), (4, 25), (8, 25), (1, 25), (5, 25), (9, 25)}
  spill event4: (0, 1, 3) {(2, 25), (6, 25), (3, 25), (7, 25)}  last

  DS 
  on spill event3 arrival: 
  Stage 0: ...
  Stage 1: (0, 1)
M1: Partition[0](:= 0 mod 4) (0, 25), (4, 25), (8, 25)
    Partition[1](:= 1 mod 4) (1, 25), (5, 25), (9, 25)

  on spill event4 arrival:
  Stage 0: ...
  Stage 1: (0, 1)
M1: Partition[0](:= 0 mod 4) (0, 25), (4, 25), (8, 25)
    Partition[1](:= 1 mod 4) (1, 25), (5, 25), (9, 25)
M2: Partition[2](:= 2 mod 4) (2, 25), (6, 25)
    Partition[2](:= 3 mod 4) (3, 25), (7, 25)

  ready event5 (0, 1) P1 M1
  ready event6 (0, 1) P2 M1
  ready event7 (0, 1) P3 M2
  ready event8 (0, 1) P4 M2 last

  ES
  on ready event5 arrival
  collect (0, 1): incomplete, M1: {P1}
  on ready event6 arrival
  collect (0, 1): incomplete, M1: {P1, P2}
  on ready event7 arrival
  collect (0, 1): incomplete, M1: {P1, P2}, M2: {P3}
  collect (0, 1): complete, M1: {P1, P2}, M2: {P3, P4}

  new runnable stages: Stage2
  Stage2
  M1
    task4
      -input: P1, P2
  M2
    task5
      -input: P3, P4

  end stage (no spill event)
 */
public class ExecuteService {

  private static Logger LOG = Logger.getLogger(StageDag.class.getName());
  private Cluster cluster_;
  private InterJobScheduler interJobScheduler_;
  private IntraJobScheduler intraJobScheduler_;
  private Queue<BaseDag> runningJobs_;
  private Queue<BaseDag> completedJobs_;
  private int nextId_;
  private int maxPartitionsPerTask_;

  private Map<Integer, Map<Integer, Double>> taskOutputs_;  // (taskId, (key, size))
  private Map<Integer, Map<String, Integer>> dagStageNumTaskMap_;    // (dagId, stageName, num of tasks)
  private Map<Integer, Map<String, Map<Integer, Map<Integer, Partition>>>> availablePartitions_; // (dagId, stageName, machineId, paritionId, partition)

  public ExecuteService(Cluster cluster, InterJobScheduler interJobScheduler,
                        IntraJobScheduler intraJobScheduler,
                        Queue<BaseDag> runningJobs, Queue<BaseDag> completedJobs, int maxPartitionsPerTask) {
    cluster_ = cluster;
    interJobScheduler_ = interJobScheduler;
    intraJobScheduler_ = intraJobScheduler;
    runningJobs_ = runningJobs;
    completedJobs_ = completedJobs;
    maxPartitionsPerTask_ = maxPartitionsPerTask;
    nextId_ = 0;
    taskOutputs_ = new HashMap<>();
    availablePartitions_ = new HashMap<>();
    dagStageNumTaskMap_ = new HashMap<>();
  }

  private void addPartition(int dagId, String stageName, int machineId, int pid, Partition partition) {
    Map<String, Map<Integer, Map<Integer, Partition>>> smpp = null;
    Map<Integer, Map<Integer, Partition>> mpp = null;
    Map<Integer, Partition> pp = null;
    if (!availablePartitions_.containsKey(dagId)) {
      availablePartitions_.put(dagId, new HashMap<>());
    }
    smpp = availablePartitions_.get(dagId);
    if (!smpp.containsKey(stageName)) {
      smpp.put(stageName, new HashMap<>());
    }
    mpp = smpp.get(stageName);
    if (!mpp.containsKey(machineId)) {
      mpp.put(machineId, new HashMap<>());
    }
    pp = mpp.get(machineId);
    pp.put(pid, partition);
  }

  public void receiveReadyEvents(boolean needInterJobScheduling, Queue<ReadyEvent> readyEventQueue) {
    Map<Integer, Set<String>> dagRunnableStagesMap = new HashMap<>();
    Set<String> newRunnableStageNameSet = null;
    int dagId = -1;
    int taskId = -1;
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
    LOG.info("Runnable dag stages: " + dagRunnableStagesMap);

    // TODO: collect data and assign tasks
    //     1. handle data locaility
    //     2. compute the output key sizes
    for (Map.Entry<Integer, Set<String>> entry: dagRunnableStagesMap.entrySet()) {
      dagId = entry.getKey();
      StageDag dag = getDagById(dagId);
      newRunnableStageNameSet = entry.getValue();
      Map<Integer, Double> taskOutput = null;
      int totalNumTasks = 0;
      for(String stageName : newRunnableStageNameSet) {
        dag.addRunnableStage(stageName);
        Stage stage = dag.stages.get(stageName);
        int numTasks = stage.getNumTasks();
        totalNumTasks = numTasks;
        if (stage.parents.isEmpty()) {    // start stages
          double[] keySizesPerTask = Arrays.stream(dag.getInputKeySize()).map(v -> v / stage.getNumTasks()).toArray();
          while (0 < numTasks--) {
            taskId = nextId_;
            nextId_++;
            // compute output data size
            if (!taskOutputs_.containsKey(taskId)) {
              taskOutputs_.put(taskId, new HashMap<>());
            }
            taskOutput = taskOutputs_.get(taskId);
            for (int i = 0; i < keySizesPerTask.length; i++) {
              taskOutput.put(i, keySizesPerTask[i] * stage.getOutinRatio());
            }
            // add runnable task
            dag.addRunnableTask(taskId, stageName, -1);
          }
        } else {   // non-start stages
          totalNumTasks = 0;
          System.out.println("availablePartitions_=" + availablePartitions_);
          // TODO: multiple parents
          String parent = dag.stages.get(stageName).parents.entrySet().iterator().next().getKey();
          System.out.println("dagId=" + dagId + ", stageName=" + stageName + ", parent=" + parent);
          Map<Integer, Map<Integer, Partition>> machinePartMap = availablePartitions_.get(dagId).get(parent);
          for (Map.Entry<Integer, Map<Integer, Partition>> mchPart: machinePartMap.entrySet()) {
            int machineId = mchPart.getKey();
            Map<Integer, Partition> partMap = mchPart.getValue();
            int count = 0;
            for (Map.Entry<Integer, Partition> partkv: partMap.entrySet()) {
              if (count % maxPartitionsPerTask_ == 0) {
                taskId = nextId_;
                nextId_++;
                totalNumTasks++;
                if (!taskOutputs_.containsKey(taskId)) {
                  taskOutputs_.put(taskId, new HashMap<>());
                }
                taskOutput = taskOutputs_.get(taskId);
                dag.addRunnableTask(taskId, stageName, machineId);
                count++;
                // taskToDag_.add(taskId, dag);
              }
              Partition pt = partkv.getValue();
              Map<Integer, Map<Integer, Double>> data = pt.getData();   // machine, key, size
              assert data.size() == 1 && data.containsKey(machineId);  // already aggregated
              Map<Integer, Double> ksMap = data.get(machineId);
              for (Map.Entry<Integer, Double> ksPair: ksMap.entrySet()) {
                taskOutput.put(ksPair.getKey(), ksPair.getValue() * stage.getOutinRatio());
              }
            }
          }
        }
        if (!dagStageNumTaskMap_.containsKey(dagId)) {
          dagStageNumTaskMap_.put(dagId, new HashMap<>());
        }
        LOG.info("Stage: " + stageName + ", number of tasks=" + totalNumTasks);
        dagStageNumTaskMap_.get(dagId).put(stageName, totalNumTasks);
        //assume the stage duration and demands are successfully loaded at the beginning of simulator

        /* double newTaskDuration = newRunnableStage.vDuration;
        Resources newTaskRsrcDemands = new Resources(newRunnableStage.vDemands);
        Task task = new Task(dagId, newRunnableStage.id, taskId, newTaskDuration, newTaskRsrcDemands); */
      }
      schedule(dagId);
    }

    // emitSpillEvents(spillEventQueue);
  }

  private void receiveReadyEvent(ReadyEvent readyEvent, Set<String> newRunnableStageNameSet) {
    int dagId = readyEvent.getDagId();
    BaseDag dag = getDagById(dagId);
    //fetch data from the partition of this readyEvent
    int stageId = readyEvent.getStageId();
    String stageName = readyEvent.getStageName();
    Partition partition = readyEvent.getPartition();

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
    this.addPartition(dagId, stageName, id, readyEvent.getPartitionId(), partition);

    if(partition.isLastPartReady()) {
      newRunnableStageNameSet.addAll(updateRunnable(stageName, dag));
    }
  }

  Set<String> updateRunnable(String stageName, BaseDag dag) {
    StageDag stageDag = (StageDag) dag;
    stageDag.moveRunningToFinish(stageName);
    Set<String> result = stageDag.updateRunnableStages();
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

  private StageDag getDagById(int dagId) {
    BaseDag dag = null;
    for(BaseDag e : runningJobs_) {
      if(e.getDagId() == dagId) {
        dag = e;
        break;
      }
    }
    return (StageDag)dag;
  }

  public boolean finishTasks(Queue<SpillEvent> spillEventQueue) {
    boolean jobCompleted = false;
    Map<Integer, List<Integer>> finishedTasks = cluster_.finishTasks();
    for (Map.Entry<Integer, List<Integer>> entry: finishedTasks.entrySet()) {
      int dagId = entry.getKey();
      List<Integer> finishedTasksPerDag = entry.getValue();
      LOG.info("dagId: " + dagId + ", finished tasks: " + finishedTasksPerDag);
      for (Integer taskId: finishedTasksPerDag) {
        jobCompleted = jobCompleted || emit(spillEventQueue, dagId, taskId);
      }
    }
    return jobCompleted;
  }

  // return whether the dag has finished
  private boolean emit(Queue<SpillEvent> spillEventQueue, int dagId, int taskId) {
    Map<Integer, Double> data = taskOutputs_.get(taskId);
    taskOutputs_.remove(taskId);
    StageDag dag = getDagById(dagId);
    int stageId = dag.getStageIdByTaskId(taskId);
    String stageName = dag.vertexToStage.get(taskId);
    // decrease the num task until 0
    LOG.info("Current dag stage num task map:" + dagStageNumTaskMap_);
    int numRemaingTasks = dagStageNumTaskMap_.get(dagId).get(stageName) - 1;
    boolean lastSpill = false;
    if (numRemaingTasks == 0) {
      lastSpill = true;
      dagStageNumTaskMap_.get(dagId).remove(stageName);
    } else {
      dagStageNumTaskMap_.get(dagId).put(stageName, numRemaingTasks);
    }
    double timestamp = Simulator.CURRENT_TIME;
    SpillEvent spill = new SpillEvent(data, lastSpill, dagId, stageId, stageName, taskId, timestamp);
    boolean endStage = dag.stages.get(stageName).children.isEmpty();
    if (!endStage) {
      LOG.info("new spill event: " + spill);
      spillEventQueue.add(spill);
    }
    boolean jobCompleted = lastSpill && endStage;
    if (jobCompleted) {
      LOG.info("Job completed. DagId = " + dagId);
      runningJobs_.remove(dag);
      completedJobs_.add(dag);
    }
    return jobCompleted;
  }

}
