package carbyne.F2;

public class ReadyEvent {
  private int dagId_;
  private int stageId_;
  private String stageName_;
  private int partitionId_;
  private Partition partition_;

  public ReadyEvent(int dagId, int stageId, String name, int partitionId, Partition partition) {
    dagId_ = dagId;
    stageId_ = stageId;
    stageName_ = name;
    partitionId_ = partitionId;
    partition_ = partition;
  }

  public int getPartitionId() { return partitionId_; }
  public int getDagId() { return dagId_; }
  public int getStageId() { return stageId_; }
  public String getStageName() { return stageName_; }
  public Partition getPartition() { return partition_; }
  public boolean isLastPartition() { return partition_.isLastPartReady(); }
}
