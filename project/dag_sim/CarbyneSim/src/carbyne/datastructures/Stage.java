package carbyne.datastructures;

import carbyne.utils.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Stage {

  public int id;
  public String name;

  private Set<Integer> taskIds_;

  private int numTasks_;    // maximum number of tasks that can be parallelized
  private double outinRatio_;
  public double vDuration;
  public Resources vDemands;

  public Map<String, Dependency> parents, children;  // <stageName, dependency>

  public Stage(String name, int id, int numTasks, double duration,
      double[] resources, double outinRatio) {
    this.name = name;
    this.id = id;
    numTasks_ = numTasks;
    outinRatio_ = outinRatio;

    parents = new HashMap<String, Dependency>();
    children = new HashMap<String, Dependency>();
    taskIds_ = new HashSet<>();

    vDuration = duration;
    vDemands = new Resources(resources);

    // System.out.println("New Stage" + this.name + "," + this.id + "," + this.vids + ", Duration:" + vDuration + ",Demands" + vDemands); 
  }

  /* public static Stage clone(Stage stage) {
    Stage clonedStage = new Stage(stage.name, stage.id, stage.getNumTasks(),
        stage.vDuration, stage.vDemands.resources, stage.getIntermediateSize());

    clonedStage.parents = new HashMap<String, Dependency>();
    clonedStage.children = new HashMap<String, Dependency>();

    for (Map.Entry<String, Dependency> entry : stage.parents.entrySet()) {
      String stageName = entry.getKey();
      Dependency dep = entry.getValue();
      Dependency clonedDep = new Dependency(dep.parent, dep.child, dep.type,
          dep.parent_ids, dep.child_ids);
      clonedStage.parents.put(stageName, clonedDep);
    }

    for (Map.Entry<String, Dependency> entry : stage.children.entrySet()) {
      String stageName = entry.getKey();
      Dependency dep = entry.getValue();
      Dependency clonedDep = new Dependency(dep.parent, dep.child, dep.type,
          dep.parent_ids, dep.child_ids);
      clonedStage.children.put(stageName, clonedDep);
    }
    return clonedStage;
  } */

  public Resources rsrcDemandsPerTask() { return vDemands; }

  public void addTaskId(int tid) {
    taskIds_.add(tid);
  }

  public int getNumTasks() { return numTasks_; }

  public double getOutinRatio() { return outinRatio_; }

  public Resources totalWork() {
    Resources totalWork = Resources.clone(vDemands);
    totalWork.multiply(numTasks_);
    return totalWork;
  }

  public Resources totalWorkInclDur() {
    Resources totalWork = Resources.clone(vDemands);
    totalWork.multiply(numTasks_ * vDuration);
    return totalWork;
  }

  public double stageContribToSrtfScore(Set<Integer> consideredTasks) {
    Set<Integer> stageTasks = new HashSet<Integer>();
    for (Integer task: taskIds_) {
      stageTasks.add(task);
    }
    stageTasks.removeAll(consideredTasks);

    int remTasksToSched = stageTasks.size();
    if (remTasksToSched == 0) {
      return 0;
    }
    double l2Norm = Resources.l2Norm(vDemands);
    return l2Norm * remTasksToSched * vDuration;
  }

  // RG: not optimal at all
  // for every stage, pass the entire list of running/finished tasks
  public double remTasks(Set<Integer> consideredTasks) {
    Set<Integer> stageTasks = new HashSet<Integer>();
    for (Integer task: taskIds_) {
      stageTasks.add(task);
    }
    stageTasks.removeAll(consideredTasks);
    return stageTasks.size();
  }
  // end task level convenience
}
