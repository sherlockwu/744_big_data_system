package carbyne.datastructures;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import carbyne.utils.Interval;

public abstract class BaseDag {

  public int dagId;
  public int timeArrival;

  public Map<Integer, Double> CPlength, BFSOrder;

  public abstract void setCriticalPaths();
  public abstract double totalWorkJob();
  public abstract double getMaxCP();
  public abstract Map<Integer, Double> area();
  
  public abstract double longestCriticalPath(int taskId);

  public abstract void setBFSOrder();

  public abstract Resources rsrcDemands(int task_id);

  public abstract double duration(int task_id);

  public abstract List<Interval> getChildren(int task_id);

  public abstract List<Interval> getParents(int task_id);

  public abstract Set<Integer> allTasks();

  public Resources rsrcQuota;
  public Resources rsrcInUse;

  public LinkedHashSet<Integer> runnableTasks;
  public LinkedHashSet<Integer> runningTasks;
  public LinkedHashSet<Integer> finishedTasks;

  public LinkedHashSet<Integer> launchedTasksNow;

  public double jobStartTime, jobEndTime;
  public double jobExpDur;

  // keep track remaining time from current time given some share
  public double timeToComplete;

  public BaseDag(int id, int... arrival) {
    this.dagId = id;
    this.timeArrival = (arrival.length > 0) ? arrival[0] : 0;

    rsrcQuota = new Resources();
    rsrcInUse = new Resources();

    runnableTasks = new LinkedHashSet<Integer>();
    runningTasks = new LinkedHashSet<Integer>();
    finishedTasks = new LinkedHashSet<Integer>();

    launchedTasksNow = new LinkedHashSet<Integer>();
  }

  public Resources currResDemand() {
    Resources usedRes = new Resources(0.0);
    for (int taskId : runningTasks) {
      usedRes.sum(rsrcDemands(taskId));
    }
    return usedRes;
  }
}
