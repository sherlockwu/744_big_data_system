package carbyne.datastructures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import carbyne.utils.Interval;

public class Stage {

  public int id;
  public String name;

  public Interval vids;

  public double vDuration;
  public Resources vDemands;

  public Map<String, Dependency> parents, children;

  public Stage(String name, int id, Interval vids, double duration,
      double[] resources) {
    this.name = name;
    this.id = id;
    this.vids = new Interval(vids.begin, vids.end);

    parents = new HashMap<String, Dependency>();
    children = new HashMap<String, Dependency>();

    vDuration = duration;
    vDemands = new Resources(resources);
  }

  public static Stage clone(Stage stage) {
    Stage clonedStage = new Stage(stage.name, stage.id, stage.vids,
        stage.vDuration, stage.vDemands.resources);

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
  }

  // task level convenience
  public double duration(int task) {
    assert (task >= vids.begin && task <= vids.end);
    return vDuration;
  }

  public Resources rsrcDemands(int task) {
    assert (task >= vids.begin && task <= vids.end);
    return vDemands;
  }

  public Resources totalWork() {
    Resources totalWork = Resources.clone(vDemands);
    totalWork.multiply(vids.end - vids.begin + 1);
    return totalWork;
  }

  public Resources totalWorkInclDur() {
    Resources totalWork = Resources.clone(vDemands);
    totalWork.multiply((vids.end - vids.begin + 1) * vDuration);
    return totalWork;
  }

  public double stageContribToSrtfScore(Set<Integer> consideredTasks) {
    Set<Integer> stageTasks = new HashSet<Integer>();
    for (int task = vids.begin; task <= vids.end; task++) {
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
    for (int task = vids.begin; task <= vids.end; task++) {
      stageTasks.add(task);
    }
    stageTasks.removeAll(consideredTasks);
    return stageTasks.size();
  }
  // end task level convenience
}
