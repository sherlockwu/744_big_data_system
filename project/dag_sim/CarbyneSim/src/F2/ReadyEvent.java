package carbyne.F2;

import java.util.List;

public class ReadyEvent {
  private int dagId_;
  private int stageId_;
  private int partitionId_;
  private Partition partition_;
  private boolean lastPartition_;

  public ReadyEvent(int dagId, int stageId, int partitionId, Partition partition, boolean lastPart) {
    dagId_ = dagId;
    stageId_ = stageId;
    partitionId_ = partitionId;
    partition_ = partition;
    lastPartition_ = lastPart;
  }

  public int getPartitionId() { return partitionId_; }
  public int getDagId() { return dagId_; }
  public int getStageId() { return stageId_; }
  public Partition getPartition() { return partition_; }
  public boolean isLastPartition() { return lastPartition_; }
}
