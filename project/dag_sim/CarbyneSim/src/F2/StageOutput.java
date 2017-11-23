package carbyne.F2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

class StageOutput {
  private int numMachines_;
  private int[] machineIds_;
  private int numGlobalPart_;
  private int numTotalPartitions_;
  private Map<Integer, Integer> partitionToMachine_;  // for spreading data
  private Set<Integer> readyPartitionSet_;
  private Partition[] partitions_;

  public StageOutput(double[] usage, double quota, int numGlobalPart) {
    setNumMachines(usage, quota);
    assignMachines(usage);
    numGlobalPart_ = numGlobalPart;
    numTotalPartitions_ = numGlobalPart * numMachines_;
    partitions_ = new Partition[numTotalPartitions_];
    partitionToMachine_ = new HashMap<>();
    readyPartitionSet_ = new HashSet<>();
  }

  public void setNumMachines(double[] usage, double quota) {
    numMachines_ = 2;
    for (int i = 0; i < usage.length; i++) {
      if (usage[i] < 0.75 * quota) numMachines_++;
      if (numMachines_ >= 0.15 * usage.length) break;
    }
  }

  public void assignMachines(double[] usage) {
    Integer[] indexes = IntStream.range(0, 100).boxed().toArray(Integer[]::new);
    Arrays.sort(indexes, new Comparator<Integer>() {
      @Override public int compare(final Integer i, final Integer j) {
        return Double.compare(usage[i], usage[j]);
      }
    });
    for (int i = 0; i < numMachines_; i++) { machineIds_[i] = indexes[i]; }
  }

  public Map<Integer, Double> materialize(Map<Integer, Double> data) {
    Map<Integer, Double> usage = new HashMap<>();
    int machineId = -1;
    int partId = -1;
    for (Map.Entry<Integer, Double> entry: data.entrySet()) {
      partId = entry.getKey() % numTotalPartitions_;
      if (partitionToMachine_.containsKey(partId)) {
        machineId = partitionToMachine_.get(partId);
      } else {
        machineId = machineIds_[partId / numGlobalPart_];
      }
      partitions_[partId].materialize(entry.getKey(), entry.getValue(), machineId, false);
      if (!usage.containsKey(machineId)) {
        usage.put(machineId, 0.0);
      }
      usage.put(machineId, Double.valueOf(entry.getValue().doubleValue() + usage.get(machineId).doubleValue()));
    }
    return usage;
  }

  public void markComplete() {
    for (int i = 0; i < partitions_.length; i++) {
      partitions_[i].setComplete();
    }
  }

  public Map<Integer, Partition> getReadyPartitions() {
    Map<Integer, Partition> readyParts = new HashMap<>();
    for (int i = 0; i < partitions_.length; i++) {
      if (partitions_[i].isComplete() && !readyPartitionSet_.contains(i)) { 
        readyParts.put(i, partitions_[i]);
        readyPartitionSet_.add(i);
        if (readyPartitionSet_.size() == numTotalPartitions_) {
          partitions_[i].setLastReady();
        }
      }
    }
    return readyParts;
  }
}
