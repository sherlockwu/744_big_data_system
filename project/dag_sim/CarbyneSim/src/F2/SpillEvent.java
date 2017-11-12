package carbyne.F2;

import java.util.Arrays;

public class SpillEvent {
  private double[] partitionSizes_;  // size of intermediate data in each partition
  private boolean lastSpill_;
  private int stageId_;
  private int taskId_;

  public SpillEvent(double[] partitionSizes, boolean lastSpill,
      int stageId, int taskId) {
    partitionSizes_ = Arrays.copyOf(partitionSizes, partitionSizes.length);
    lastSpill_ = lastSpill;
    stageId_ = stageId;
    taskId_ = taskId;
  }

  double[] getPartitionSizes() { return partitionSizes_; }
  boolean isLastSpill() { return lastSpill_; }
  int getStageId() { return stageId_; }
  int getTaskId() { return taskId_; }
}
