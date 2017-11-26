package carbyne.F2;

import java.util.Arrays;
import java.util.Map;

public class SpillEvent {
  Map<Integer, Double> data_; // size of intermediate data of each key
  private boolean lastSpill_;
  private int dagId_;
  private int stageId_;
  private int taskId_;
  private double timestamp_;

  public SpillEvent(Map<Integer, Double> data, boolean lastSpill,
      int dagId, int stageId, int taskId, double timestamp) {
    data_ = data;
    lastSpill_ = lastSpill;
    dagId_ = dagId;
    stageId_ = stageId;
    taskId_ = taskId;
    timestamp_ = timestamp;
  }

  Map<Integer, Double> getData() { return data_; }
  boolean isLastSpill() { return lastSpill_; }
  int getStageId() { return stageId_; }
  int getTaskId() { return taskId_; }
  int getDagId() { return dagId_; }
  double getTimestamp() { return timestamp_; }
}