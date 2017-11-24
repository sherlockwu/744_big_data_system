package carbyne.F2;

import java.util.HashMap;
import java.util.Map;

public class Partition {
  private Map<Integer, Map<Integer, Double>> machineKeySize_;    // <machineId, (key, size)>
  private double totalSize_;
  private double increaseRate_;
  private boolean complete_;
  private boolean lastPartReady_;
  private double lastUpdateTime_;
  private static double TIMETOL=0.001;
  private int activeMachineId_;
  
  public Partition() {
    machineKeySize_ = new HashMap<Integer, Map<Integer, Double>>();
    totalSize_ = 0;
    complete_ = false;
    lastPartReady_ = false;
    lastUpdateTime_ = -100.0;
    increaseRate_ = 0.0;
    activeMachineId_ = -1;
  }

  public void materialize(Integer key, double size, int machineId, boolean lastData, double time) {
    activeMachineId_ = machineId;
    if (!machineKeySize_.containsKey(machineId)) {
      machineKeySize_.put(machineId, new HashMap<Integer, Double>());
    }
    Map<Integer, Double> keySize_ = machineKeySize_.get(machineId);
    if (!keySize_.containsKey(key)) {
      keySize_.put(key, 0.0);
    }
    keySize_.put(key, Double.valueOf(size + keySize_.get(key).doubleValue()));
    if (lastUpdateTime_ + TIMETOL < time) {
      lastUpdateTime_ = time;
      increaseRate_ = size;
    } else {
      increaseRate_ += size;
    }
    totalSize_ += size;
    complete_ = lastData;
  }

  public boolean isLastPartReady() { return lastPartReady_; }
  public boolean isComplete() { return complete_; }
  public void setComplete() { complete_ = true; }
  public void setLastReady() { lastPartReady_ = true; }
  public double getIncreaseRate(int machineId) { 
    return machineId == activeMachineId_ ? increaseRate_ : 0.0;
  }
  public Map<Integer, Map<Integer, Double>> getData() { return machineKeySize_; }
  public double getPartitionSizeOnMachine(int machineId) {
    if (machineKeySize_.containsKey(machineId)) {
      return machineKeySize_.get(machineId).values().stream().mapToDouble(v -> v).sum();
    } else {
      return 0.0;
    }
  }
}
