package carbyne.datastructures;

import java.util.HashMap;
import java.util.Map;

public class Task implements Comparable<Task> {

  public int taskId;
  public int dagId;
  public double taskDuration;
  public Resources resDemands;
  Map<Integer, Double> keyShare;

  public Task(int dagId, int taskId) {
    this.dagId = dagId;
    this.keyShare = new HashMap<>();
    this.taskId = taskId;
  }

  public Task(int dagId, int taskId, double taskDuration, Resources resDemands) {
    this.dagId = dagId;
    this.taskId = taskId;
    this.taskDuration = taskDuration;
    this.resDemands = resDemands;
    this.keyShare = new HashMap<>();
  }

  public Task(double taskDuration, Resources resDemands) {
    this.taskDuration = taskDuration;
    this.resDemands = resDemands;
    this.keyShare = new HashMap<>();
  }

  @Override
  public String toString() {
    String output = "<" + this.dagId + " " + " " + this.taskId + ">";
    return output;
  }

  @Override
  public int compareTo(Task arg0) {
    return 0;
  }
}
