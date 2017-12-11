package carbyne.F2;

import java.util.logging.Logger;

import carbyne.datastructures.BaseDag;
import java.util.*;

public class DataService {
  private double[] quota_;  // quota per job
  private int numMachines_;
  private int numGlobalPart_;
  private Map<Integer, double[]> usagePerJob_;   // <DagId, list<usage>>
  private Map<Integer, Map<Integer, StageOutput>> stageOutputPerJob_;  // <DagId, <StageId, intermediate data>>

  private static Logger LOG = Logger.getLogger(DataService.class.getName());

  public DataService(double[] quota, int numGlobalPart, int numMachines, List<List<List<Integer>>> deployment) {
    quota_ = quota;
    numGlobalPart_ = numGlobalPart;
    numMachines_ = numMachines;
    usagePerJob_ = new HashMap<>();
    stageOutputPerJob_ = new HashMap<>();
    loadScheme(deployment);
  }

  public DataService(double[] quota, int numGlobalPart, int numMachines) {
    quota_ = quota;
    numGlobalPart_ = numGlobalPart;
    numMachines_ = numMachines;
    usagePerJob_ = new HashMap<>();
    stageOutputPerJob_ = new HashMap<>();
  }

  public void loadScheme(List<List<List<Integer>>> deployment) {
    for (int dagId = 0; dagId < deployment.size(); dagId++) {
      List<List<Integer>> depDag = deployment.get(dagId);
      usagePerJob_.put(dagId, new double[numMachines_]);
      stageOutputPerJob_.put(dagId, new HashMap<Integer, StageOutput>());
      Map<Integer, StageOutput> dagIntermediateData = stageOutputPerJob_.get(dagId);
      for (int stageId = 0; stageId < depDag.size(); stageId++) {
        dagIntermediateData.put(stageId, new StageOutput(usagePerJob_.get(dagId), quota_[dagId], numGlobalPart_, depDag.get(stageId)));
      }
    }
  }

  public void removeCompletedJobs(Queue<BaseDag> completedJobs) {
    for (BaseDag dag: completedJobs) {
      if (usagePerJob_.containsKey(dag.dagId)) {
        LOG.fine("Remove completed dag " + dag.dagId + " data from DS");
        usagePerJob_.remove(dag.dagId);
        stageOutputPerJob_.remove(dag.dagId);
      }
    }
  }

  public void receiveSpillEvents(Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    SpillEvent event = spillEventQueue.poll();
    Map<Integer, Partition> readyParts = null;
    while (event != null) {
      LOG.info(String.format("Receive spill event %d, %s, %d", event.getDagId(), event.getStageName(), event.getTaskId()));
      readyParts = receiveSpillEvent(event);
      for (Map.Entry<Integer, Partition> part: readyParts.entrySet()) {
        readyEventQueue.add(new ReadyEvent(event.getDagId(), event.getStageId(), event.getStageName(), part.getKey(), part.getValue()));
      }
      event = spillEventQueue.poll();
    }
  }

  public Map<Integer, Partition> receiveSpillEvent(SpillEvent event) {
    int dagId = event.getDagId();
    int stageId = event.getStageId();
    if (!usagePerJob_.containsKey(dagId)) {
      usagePerJob_.put(dagId, new double[numMachines_]);
    }
    if (!stageOutputPerJob_.containsKey(dagId)) {
      stageOutputPerJob_.put(dagId, new HashMap<Integer, StageOutput>());
    }
    Map<Integer, StageOutput> dagIntermediateData = stageOutputPerJob_.get(dagId);
    if (!dagIntermediateData.containsKey(stageId)) {
      dagIntermediateData.put(stageId, new StageOutput(usagePerJob_.get(dagId), quota_[dagId], numGlobalPart_, null));
    }
    //machineId -> usage on that machine
    Map<Integer, Double> newUsage = dagIntermediateData.get(stageId).materialize(event.getData(), event.getTimestamp());
    for (Map.Entry<Integer, Double> entry: newUsage.entrySet()) {
      usagePerJob_.get(dagId)[entry.getKey()] += entry.getValue();
    }

    // TODO: spread data
    double[] usage = usagePerJob_.get(dagId);
    int machineChosen = -1;
    Map<Integer, Set<Integer>> stagePartsToSpread = new HashMap<>();
    Set<Integer> partsToSpreadSet = null;
    Set<Integer> usedMachinesStageSet = null;
    Set<Integer> usedMachinesJobSet = new HashSet<>();
    Comparator<Integer> cmp = new Comparator<Integer>() {
      @Override public int compare(final Integer i, final Integer j) {
        return Double.compare(quota_[dagId] - usage[i], quota_[dagId] - usage[j]);
      }
    };
    for (int i = 0; i < numMachines_; i++) {
      if (usage[i] > quota_[dagId]) {
        String msg = String.format("Dag %d, data usage on machine %d (%f) exceed the quota %f", dagId, i, usage[i], quota_[dagId]);
        LOG.severe(msg);
        throw new RuntimeException(msg);
      }
      if (usage[i] > 0.75 * quota_[dagId]) {
        // never goes into this code path
        LOG.warning(String.format("Dag %d use %f storage of the machine %d, which is above 75%% of the quota %f", dagId, usage[i], i, quota_[dagId]));
        // Choose partitions to spread
        for (Map.Entry<Integer, StageOutput> entry: dagIntermediateData.entrySet()) {
          partsToSpreadSet = entry.getValue().choosePartitionsToSpread(i);
          stagePartsToSpread.put(entry.getKey(), partsToSpreadSet);
          usedMachinesStageSet = entry.getValue().getUsedMachines();
          usedMachinesJobSet.addAll(usedMachinesStageSet);
        }
        // remove machines that already saturated
        for (int j = 0; j < numMachines_; j++) {
          if (usage[j] > 0.75 * quota_[dagId]) {
            usedMachinesStageSet.remove(j);
            usedMachinesJobSet.remove(j);
          }
        }
        // Choose machines to hold the spreaded partitions
        for (Map.Entry<Integer, Set<Integer>> entry: stagePartsToSpread.entrySet()) {
          stageId = entry.getKey();
          partsToSpreadSet = entry.getValue();
          if (!usedMachinesStageSet.isEmpty()) {
            machineChosen = Collections.max(usedMachinesStageSet, cmp);
          } else if (!usedMachinesJobSet.isEmpty()) {
            machineChosen = Collections.max(usedMachinesJobSet, cmp);
          }
          else {
            for (int j = 0; j < numMachines_; j++) {
              if (usage[j] <= 0.75 * quota_[dagId]) {
                machineChosen = j;
                break;
              }
            }
          }
          for (Integer partitionId : partsToSpreadSet) {
            LOG.warning(String.format("move stage %d, parition %d to machine %d", stageId, partitionId, machineChosen));
            dagIntermediateData.get(stageId).spreadPartition(partitionId, machineChosen);
          }
        }
      }
    }
    // mark complete
    if (event.isLastSpill()) {
       dagIntermediateData.get(stageId).markComplete();
    }
    LOG.fine("Dag " + dagId + " data usage: [" + Arrays.toString(usage) + "], quota:" + quota_[dagId]);
    return dagIntermediateData.get(stageId).getReadyPartitions();
  }
}
