package carbyne.F2;

import java.util.HashMap;
import java.util.Map;

public class Partition {
  private Map<Integer, Map<Integer, Double>> sizeByKey_;    // <key, (machineId, size)>
  private boolean complete_;
  private boolean lastPartReady_;
  
  public Partition() {
    sizeByKey_ = new HashMap<Integer, Map<Integer, Double>>();
    complete_ = false;
    lastPartReady_ = false;
  }

  public void materialize(Integer key, double size, int machineId, boolean lastData) {
    if (!sizeByKey_.containsKey(key)) {
      sizeByKey_.put(key, new HashMap<Integer, Double>());
    }
    if (!sizeByKey_.get(key).containsKey(machineId)) {
      sizeByKey_.get(key).put(machineId, 0.0);
    }
    Map<Integer, Double> keyOnMachines_ = sizeByKey_.get(key);
    keyOnMachines_.put(machineId, Double.valueOf(size + keyOnMachines_.get(machineId).doubleValue()));
    complete_ = lastData;
  }

  public boolean isLastPartReady() { return lastPartReady_; }
  public boolean isComplete() { return complete_; }
  public void setComplete() { complete_ = true; }
  public void setLastReady() { lastPartReady_ = true; }
}
