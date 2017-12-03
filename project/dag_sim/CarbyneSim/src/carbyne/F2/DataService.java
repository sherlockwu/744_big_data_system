package carbyne.F2;

import java.util.*;
import java.util.logging.Logger;

public class DataService {
  private double[] quota_;  // quota per job
  private int numMachines_;
  private int numGlobalPart_;
  private Map<Integer, double[]> usagePerJob_;   // <DagId, list<usage>>
  private Map<Integer, Map<Integer, StageOutput>> stageOutputPerJob_;  // <DagId, <StageId, intermediate data>>

  private static Logger LOG = Logger.getLogger(DataService.class.getName());

  public DataService(double[] quota, int numGlobalPart, int numMachines) {
    quota_ = quota;
    numGlobalPart_ = numGlobalPart;
    numMachines_ = numMachines;
    usagePerJob_ = new HashMap<>();
    stageOutputPerJob_ = new HashMap<>();
  }

  public void receiveSpillEvents(Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    SpillEvent event = spillEventQueue.poll();
    Map<Integer, Partition> readyParts = null;
    while (event != null) {
      LOG.info("Receive spill event: " + event);
      readyParts = receiveSpillEvent(event);
      for (Map.Entry<Integer, Partition> part: readyParts.entrySet()) {
        readyEventQueue.add(new ReadyEvent(event.getDagId(), event.getStageId(), event.getStageName(), part.getKey(), part.getValue()));
      }
      event = spillEventQueue.poll();
    }
  }

  public Map<Integer, Partition> receiveSpillEvent(SpillEvent event) {
    // Map<Integer, Partition> readyParts = new HashMap<>();
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
      dagIntermediateData.put(stageId, new StageOutput(usagePerJob_.get(dagId), quota_[dagId], numGlobalPart_));
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
    for (int i = 0; i < usage.length; i++) {
      if (usage[i] > 0.75 * quota_[dagId]) {
        // Choose partitions to spread
        for (Map.Entry<Integer, StageOutput> entry: dagIntermediateData.entrySet()) {
          partsToSpreadSet = entry.getValue().choosePartitionsToSpread(i);
          stagePartsToSpread.put(entry.getKey(), partsToSpreadSet);
          usedMachinesStageSet = entry.getValue().getUsedMachines();
          usedMachinesJobSet.addAll(usedMachinesStageSet);
        }
        // Choose machines to hold the spreaded partitions
        for (Map.Entry<Integer, Set<Integer>> entry: stagePartsToSpread.entrySet()) {
          stageId = entry.getKey();
          partsToSpreadSet = entry.getValue();
          machineChosen = Collections.max(usedMachinesStageSet, cmp);
          if (quota_[dagId] - usage[machineChosen] <= 0) {
            machineChosen = Collections.max(usedMachinesJobSet, cmp);
          }
          if (quota_[dagId] - usage[machineChosen] <= 0) {
            for (int j = 0; j < numMachines_; j++) {
              if (!usedMachinesJobSet.contains(j)) {
                machineChosen = j;
                break;
              }
            }
          }
          for (Integer partitionId : partsToSpreadSet) {
            dagIntermediateData.get(stageId).spreadPartition(partitionId, machineChosen);
          }
        }
      }
    }
    // mark complete
    if (event.isLastSpill()) {
       dagIntermediateData.get(stageId).markComplete();
    }
    return dagIntermediateData.get(stageId).getReadyPartitions();
  }
}
