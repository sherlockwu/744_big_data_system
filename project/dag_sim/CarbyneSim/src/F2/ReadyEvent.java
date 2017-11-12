package carbyne.F2;

import java.util.List;

public class ReadyEvent {
  private int gPartitionId_;
  private List<Integer> partitionLocs_;   // machines
  private boolean lastPartition_;

  public ReadyEvent(int id, List<Integer> partitionLocs, boolean lastPart) {
    gPartitionId_ = id;
    partitionLocs_ = partitionLocs;
    lastPartition_ = lastPart;
  }

  public int getPartitionId() { return gPartitionId_; }
  public List<Integer> getPartitionLocs() { return partitionLocs_; }
  public boolean isLastPartition() { return lastPartition_; }
}
