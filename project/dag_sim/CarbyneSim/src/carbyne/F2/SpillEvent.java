package carbyne.F2;

import java.util.Map;

public class SpillEvent {
  Map<Integer, Double> data_; // size of intermediate data of each key
  private boolean lastSpill_;
  private int dagId_;
  private int stageId_;
  private String stageName_;
  private int taskId_;
  private double timestamp_;

  public SpillEvent(Map<Integer, Double> data, boolean lastSpill,
      int dagId, int stageId, String stageName, int taskId, double timestamp) {
    data_ = data;
    lastSpill_ = lastSpill;
    dagId_ = dagId;
    stageId_ = stageId;
    stageName_ = stageName;
    taskId_ = taskId;
    timestamp_ = timestamp;
  }

  public Map<Integer, Double> getData() { return data_; }
  public boolean isLastSpill() { return lastSpill_; }
  public int getStageId() { return stageId_; }
  public int getTaskId() { return taskId_; }
  public int getDagId() { return dagId_; }
  public String getStageName() { return stageName_; }
  public double getTimestamp() { return timestamp_; }
  public void setTimestamp(double timestamp) { timestamp_ = timestamp; }

  public String toString() {
    return String.format("dagId: %d, stageName: %s, taskId: %d", dagId_, stageName_, taskId_) + ", data: " + data_ + ", lastSpill: " + lastSpill_; 
  }
}
