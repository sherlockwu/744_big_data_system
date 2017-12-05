package carbyne.datastructures;

import java.util.logging.Logger;

import carbyne.cluster.Cluster;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Simulator;
import carbyne.utils.Interval;
import java.util.*;

public class StageDag extends BaseDag {

  private static Logger LOG = Logger.getLogger(StageDag.class.getName());
  public String dagName;
  private double quota_;
  private double[] inputKeySizes_;

  public Map<String, Stage> stages;
  public Map<Integer, String> vertexToStage;  // <vertexId (taskID), stageName vertexId in>

  private Set<String> runnableStages_;
  private Set<String> runningStages_;
  private Set<String> finishedStages_;
  private Map<Integer, Integer> taskToMachine_; // assign certain task to machine. set -1 if no preference

  // keep track of ancestors and descendants of tasks per task
  public Map<Integer, Set<Integer>> ancestorsT, descendantsT,
      unorderedNeighborsT;
  public Map<String, Set<String>> ancestorsS, descendantsS,
      unorderedNeighborsS;

  public Set<String> chokePointsS;
  public Set<Integer> chokePointsT;

  // keep track of adjusted profiles for certain tasks;
  public Map<Integer, Task> adjustedTaskDemands = null;

  public StageDag(String dagName, int id, double quota, double[] inputKeySizes, double... arrival) {
    super(id, arrival);
    stages = new HashMap<String, Stage>();
    chokePointsS = new HashSet<String>();
    chokePointsT = null;
    this.dagName = dagName;
    quota_ = quota;
    runnableStages_ = new HashSet<>();
    runningStages_ = new HashSet<>();
    finishedStages_ = new HashSet<>();
    inputKeySizes_ = inputKeySizes;
    taskToMachine_ = new HashMap<>();
  }

  public double getQuota() {return quota_; }

  public double[] getInputKeySize() {return inputKeySizes_; }

  public boolean isRunnableStage(String name) { return runnableStages_.contains(name); }
  public boolean isRunningStage(String name) { return runningStages_.contains(name); }
  public boolean isFinishedStage(String name) { return finishedStages_.contains(name); }

  public void addRunnableStage(String name) {
    if (stages.containsKey(name)) {
      runnableStages_.add(name);
    } else {
      LOG.severe(name + " is not a valid stage in the current job (" + dagName + ")");
    }
  }

  public void moveRunnableToRunning(String name) {
    if (runnableStages_.contains(name)) {
      runnableStages_.remove(name);
      runningStages_.add(name);
    } else {
      LOG.severe(name + " is not a valid runnable stage in the current job (" + dagName + ")");
    }
  }

  public void moveRunningToFinish(String name) {
    if (runningStages_.contains(name)) {
      runningStages_.remove(name);
      finishedStages_.add(name);
    } else {
      LOG.severe(name + " is not a valid running stage in the current job (" + dagName + ")");
    }
  }

  public Set<String> updateRunnableStages() {
    Set<String> newRunnableSet = new HashSet<>();
    for (Map.Entry<String, Stage> entry: stages.entrySet()) {
      String name = entry.getKey();
      if (runnableStages_.contains(name) ||
          runningStages_.contains(name) ||
          finishedStages_.contains(name)) continue;
      if (finishedStages_.containsAll(entry.getValue().parents.keySet())) {
        runnableStages_.add(name);
        newRunnableSet.add(name);
      }
    }
    return newRunnableSet;
  }

  public Set<String> setInitiallyRunnableStages() {
    LOG.info("set initially runnable stages for dag " + dagId);
    Set<String> newRunnableSet = new HashSet<>();
    for (Map.Entry<String, Stage> entry: stages.entrySet()) {
      String name = entry.getKey();
      if (!runnableStages_.contains(name) &&
          !runningStages_.contains(name) &&
          !finishedStages_.contains(name) &&
          entry.getValue().parents.isEmpty()) {
        newRunnableSet.add(name);
        runnableStages_.add(name);
      }
    }
    return newRunnableSet;
  }

  // only for tasks that are not running or finished
  public void reverseDag() {
    // remove tasks which are finished or running
    if (runningTasks != null) {
      for (int taskId : runningTasks) {
        this.vertexToStage.remove(taskId);
      }
    }
    if (finishedTasks != null) {
      for (int taskId : finishedTasks) {
        this.vertexToStage.remove(taskId);
      }
    }
    runningTasks.clear();
    runnableTasks.clear();

    // if any predecessor is still running -> can't consider it
    for (int taskId : this.vertexToStage.keySet()) {
      if (this.descendantsT.get(taskId).isEmpty()/* && (ancestors.isEmpty()) */) {
        runnableTasks.add(taskId);
      }
    }
    finishedTasks.clear();
  }

  public Set<Integer> chokePointsT() {
    if (chokePointsT == null) {
      chokePointsT = new HashSet<Integer>();

      for (int task : this.vertexToStage.keySet()) {
        if (isChokePoint(task)) {
          chokePointsT.add(task);
        }
      }
    }
    return chokePointsT;
  }

  public boolean isChokePoint(int taskId) {
    String stageTaskId = this.vertexToStage.get(taskId);
    return (this.chokePointsS.contains(stageTaskId));
  }

  // view dag methods //
  public void viewDag() {
    System.out.println("\n == DAG: " + this.dagId + " ==");

    for (Stage stage : stages.values()) {
      System.out.print("Stage: " + stage.id + " "+stage.name + ", duration:" + stage.vDuration + ", ");
      System.out.println(stage.vDemands);

      System.out.print("Maximum Parallel Tasks:" + stage.getNumTasks());

      System.out.print("  Parents: ");
      for (String parent : stage.parents.keySet())
        System.out.print(parent + ", ");
      System.out.println();
    }

    System.out.println("== CP ==");
    System.out.println(CPlength);
  }

  public void populateParentsAndChildrenStructure(String stage_src,
      String stage_dst, String comm_pattern) {

    if (!stages.containsKey(stage_src) || !stages.containsKey(stage_dst)) {
      LOG.severe("A stage entry for " + stage_src + " or " + stage_dst
          + " should be already inserted !!!");
      return;
    }
    if (stages.get(stage_src).children.containsKey(stage_dst)) {
      LOG.severe("An edge b/w " + stage_src + " and " + stage_dst
          + " is already present.");
      return;
    }
    Dependency d = new Dependency(stage_src, stage_dst, comm_pattern);
    //    stages.get(stage_src).vids, stages.get(stage_dst).vids);

    stages.get(stage_src).children.put(stage_dst, d);
    stages.get(stage_dst).parents.put(stage_src, d);
  }

  public void addRunnableTask(int taskId, String stageName, int machineId) {
    this.runnableTasks.add(taskId);
    this.vertexToStage.put(taskId, stageName);
    this.taskToMachine_.put(taskId, machineId);
  }

  public int getAssignedMachine(int taskId) {
    return taskToMachine_.get(taskId);
  }
  // end read dags from file //

  // DAG traversals //
  @Override
  public void setCriticalPaths() {
    if (CPlength == null) {
      CPlength = new HashMap<>();
    }
    for (String stageName : stages.keySet()) {
      longestCriticalPath(stageName);
    }
  }

  @Override
  public double getMaxCP() {
    return Collections.max(CPlength.values());
  }

  @Override
  public double longestCriticalPath(String stageName) {
    if (CPlength != null && CPlength.containsKey(stageName)) {
      return CPlength.get(stageName);
    }

    if (CPlength == null) {
      CPlength = new HashMap<String, Double>();
    }

    double maxChildCP = Double.MIN_VALUE;
    // String stageName = this.vertexToStage.get(taskId);

    Set<String> childrenStages = stages.get(stageName).children.keySet();
    // System.out.println("Children: "+children);
    if (childrenStages.size() == 0) {
      maxChildCP = 0;
    } else {
      for (String chStage : childrenStages) {
        double childCP = longestCriticalPath(chStage);
        if (maxChildCP < childCP) {
          maxChildCP = childCP;
        }
      }
    }

    double cp = maxChildCP + stages.get(stageName).vDuration;
    if (!CPlength.containsKey(stageName)) {
      CPlength.put(stageName, cp);
    }

    return CPlength.get(stageName);
  }

  @Override
  public void setBFSOrder() {
    if (BFSOrder == null) {
      BFSOrder = new HashMap<Integer, Double>();
    }
    if (BFSOrder.size() == vertexToStage.size()) {
      return;
    }

    Set<String> visitedStages = new HashSet<String>();
    Map<String, Integer> numParents = new HashMap<String, Integer>();
    List<String> freeStages = new ArrayList<String>();

    for (Stage s : stages.values()) {
      if (s.parents.size() == 0) {
        freeStages.add(s.name);
      } else {
        numParents.put(s.name, s.parents.size());
      }
    }

    int currentLevel = 0;
    while (freeStages.size() > 0) {
      List<String> nextFreeStages = new ArrayList<String>();
      for (String s : freeStages) {
        assert (!visitedStages.contains(s));

        /*int sb = stages.get(s).vids.begin;
        int se = stages.get(s).vids.end; */
        int sb = 0, se = 1;  // TODO: fix BFS 
        for (int i = sb; i <= se; i++) {
          BFSOrder.put(i, (double) currentLevel);
        }

        visitedStages.add(s);
        for (String c : stages.get(s).children.keySet()) {
          int updatedVal = numParents.get(c);
          updatedVal -= 1;
          assert (updatedVal >= 0);
          numParents.put(c, updatedVal);

          if (numParents.get(c) == 0) {
            nextFreeStages.add(c);
          }
        }
      }
      freeStages = nextFreeStages;
      currentLevel++;
    }

    for (int tId : vertexToStage.keySet()) {
      double updatedVal = BFSOrder.get(tId);
      BFSOrder.put(tId, currentLevel - updatedVal);
    }
  }

  public int getStageIdByTaskId(int taskId) {
    return stages.get(vertexToStage.get(taskId)).id;
  }

  // end DAG traversals //

  @Override
  public Resources rsrcDemands(int taskId) {
    if (adjustedTaskDemands != null && adjustedTaskDemands.get(taskId) != null) {
      return adjustedTaskDemands.get(taskId).resDemands;
    }
    return stages.get(vertexToStage.get(taskId)).rsrcDemandsPerTask();
  }

  @Override
  public double duration(int taskId) {
    if (adjustedTaskDemands != null && adjustedTaskDemands.get(taskId) != null) {
      return adjustedTaskDemands.get(taskId).taskDuration;
    }
    return stages.get(vertexToStage.get(taskId)).vDuration;
  }

  @Override
  public List<Interval> getChildren(int taskId) {

    List<Interval> childrenTask = new ArrayList<Interval>();
    for (Dependency dep : stages.get(vertexToStage.get(taskId)).children
        .values()) {
      Interval i = dep.getChildren(taskId);
      childrenTask.add(i);
    }
    return childrenTask;
  }

  @Override
  public List<Interval> getParents(int taskId) {

    List<Interval> parentsTask = new ArrayList<Interval>();
    for (Dependency dep : stages.get(vertexToStage.get(taskId)).parents
        .values()) {
      Interval i = dep.getParents(taskId);
      parentsTask.add(i);
    }
    return parentsTask;
  }

  public Set<Integer> getParentsTasks(int taskId) {
    Set<Integer> allParentTasks = new HashSet<Integer>();

    for (Interval ival : getParents(taskId)) {
      for (int i = ival.begin; i <= ival.end; i++) {
        allParentTasks.add(i);
      }
    }
    return allParentTasks;
  }

  public Set<Integer> getChildrenTasks(int taskId) {
    Set<Integer> allChildrenTasks = new HashSet<Integer>();

    for (Interval ival : getChildren(taskId)) {
      for (int i = ival.begin; i <= ival.end; i++) {
        allChildrenTasks.add(i);
      }
    }
    return allChildrenTasks;
  }

  @Override
  public Set<Integer> allTasks() {
    return vertexToStage.keySet();
  }

  public int numTotalTasks() {
    int n = 0;
    for (Stage stage : stages.values()) {
      n += stage.getNumTasks();
    }
    return n;
  }

  public Set<Integer> remTasksToSchedule() {
    Set<Integer> allTasks = new HashSet<Integer>(vertexToStage.keySet());
    Set<Integer> consideredTasks = new HashSet<Integer>(this.finishedTasks);
    consideredTasks.addAll(this.runningTasks);
    allTasks.removeAll(consideredTasks);
    return allTasks;
  }

  public Resources totalResourceDemand() {
    Resources totalResDemand = new Resources(0.0);
    for (Stage stage : stages.values()) {
      totalResDemand.sum(stage.totalWork());
    }
    return totalResDemand;
  }

  public Resources totalWorkInclDur() {
    Resources totalResDemand = new Resources(0.0);
    for (Stage stage : stages.values()) {
      totalResDemand.sum(stage.totalWork());
    }
    return totalResDemand;
  }

  @Override
  public Map area() {
    Map<Integer, Double> area_dims = new TreeMap<Integer, Double>();
    for (Stage stage : stages.values()) {
      for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
        double bef = area_dims.get(i) != null ? area_dims.get(i) : 0;
        bef += stage.getNumTasks() * stage.vDuration * stage.vDemands.resources[i];
        area_dims.put(i, bef);
      }
    }
    return area_dims;
  }

  @Override
  public double totalWorkJob() {
    double scoreTotalWork = 0;
    for (Stage stage : stages.values()) {
      scoreTotalWork += stage.stageContribToSrtfScore(new HashSet<Integer>());
    }
    return scoreTotalWork;
  }

  public double srtfScore() {
    Set<Integer> consideredTasks = new HashSet<Integer>(this.finishedTasks);
    consideredTasks.addAll(this.runningTasks);

    double scoreSrtf = 0;
    for (Stage stage : stages.values()) {
      scoreSrtf += stage.stageContribToSrtfScore(consideredTasks);
    }
    return scoreSrtf;
  }


  // should decrease only the resources allocated in the current time quanta
  public Resources currResShareAvailable() {
    Resources totalShareAllocated = Resources.clone(this.rsrcQuota);

    for (int task: runningTasks) {
      Resources rsrcDemandsTask = rsrcDemands(task);
      totalShareAllocated.subtract(rsrcDemandsTask);
    }
    LOG.finest("Dag: " + dagId + " compute resource available." + 
    " quota: " + rsrcQuota + "available: " + totalShareAllocated + "running tasks:" + runningTasks.size());
    totalShareAllocated.normalize();  // change <0 to 0
    return totalShareAllocated;
  }

  /* public void seedUnorderedNeighbors() {

    int numTasks = this.allTasks().size();

    if (this.unorderedNeighborsT == null) {
      this.unorderedNeighborsT = new HashMap<Integer, Set<Integer>>();
    }
    if (this.ancestorsT == null) {
      this.ancestorsT = new HashMap<Integer, Set<Integer>>();
    }
    if (this.descendantsT == null) {
      this.descendantsT = new HashMap<Integer, Set<Integer>>();
    }
    if (this.unorderedNeighborsT.size() == numTasks) {
      return;
    }

    for (int i = 0; i < numTasks; i++) {
      seedAncestors(i, this.ancestorsT);
    }
    for (int i = 0; i < numTasks; i++) {
      seedDescendants(i, this.descendantsT);
    }

    List<Integer> allTasks = new ArrayList<Integer>();
    for (int i = 0; i < numTasks; i++) {
      allTasks.add(i);
    }
    for (int i = 0; i < numTasks; i++) {

      Set<Integer> union_i = new HashSet<Integer>(ancestorsT.get(i));
      union_i.addAll(descendantsT.get(i));
      Interval i_stage_ival = this.stages.get(this.vertexToStage.get(i)).vids;
      for (int j = i_stage_ival.begin; j <= i_stage_ival.end; j++) {
        union_i.add(j);
      }
      Set<Integer> unorderedNeighborsT_i = new HashSet<Integer>(allTasks);

      unorderedNeighborsT_i.removeAll(union_i);
      unorderedNeighborsT.put(i, unorderedNeighborsT_i);
    }

    this.ancestorsS = new HashMap<String, Set<String>>();
    this.descendantsS = new HashMap<String, Set<String>>();
    this.unorderedNeighborsS = new HashMap<String, Set<String>>();

    for (Stage s : stages.values()) {
      int vid = s.vids.begin;

      Set<String> ancestorsStS = new HashSet<String>();
      for (int task : this.ancestorsT.get(vid)) {
        String ancestorSt = this.vertexToStage.get(task);
        ancestorsStS.add(ancestorSt);
      }
      ancestorsStS.remove(s);
      this.ancestorsS.put(s.name, ancestorsStS);

      Set<String> descendantsStS = new HashSet<String>();
      for (int task : this.descendantsT.get(vid)) {
        String descendantSt = this.vertexToStage.get(task);
        descendantsStS.add(descendantSt);
      }
      descendantsStS.remove(s);
      this.descendantsS.put(s.name, descendantsStS);

      Set<String> unorderedNeighborsStS = new HashSet<String>();
      for (int task : this.unorderedNeighborsT.get(vid)) {
        String unorderedNeighborSt = this.vertexToStage.get(task);
        unorderedNeighborsStS.add(unorderedNeighborSt);
      }
      unorderedNeighborsStS.remove(s);
      this.unorderedNeighborsS.put(s.name, unorderedNeighborsStS);
    }

    // particular case:
    if (checkTwoStageDag())
      return;

    // compute the tasks chokepoints
    for (String s : this.unorderedNeighborsS.keySet()) {
      if (this.unorderedNeighborsS.get(s).isEmpty()) {
        this.chokePointsS.add(s);
      }
    }
    return;
  } */

  // if only two stages and have a parent / child relationship then
  // the stage with no descendants is a chokepoint as default
  public boolean checkTwoStageDag() {
    if (stages.size() == 2) {
      // this is very bad
      List<String> lstages = new ArrayList<String>();
      for (String stage : stages.keySet()) {
        lstages.add(stage);
      }
      if (ancestorsS.get(lstages.get(0)).contains(lstages.get(1))) {
        this.chokePointsS.add(lstages.get(0));
        return true;
      }
      if (ancestorsS.get(lstages.get(1)).contains(lstages.get(0))) {
        this.chokePointsS.add(lstages.get(1));
        return true;
      }
    }
    return false;
  }

  public void seedAncestors(int i, Map<Integer, Set<Integer>> ancestors) {
    if (!ancestors.containsKey(i)) {
      Set<Integer> a = new HashSet<Integer>();

      for (int x : this.getParentsTasks(i)) {
        if (!ancestors.containsKey(x))
          seedAncestors(x, ancestors);

        a.add(x);
        for (Integer y : ancestors.get(x))
          a.add(y);
      }
      ancestors.put(i, a);
    }
  }

  public void seedDescendants(int i, Map<Integer, Set<Integer>> descendants) {
    if (!descendants.containsKey(i)) {
      Set<Integer> d = new HashSet<Integer>();

      for (int x : this.getChildrenTasks(i)) {
        if (!descendants.containsKey(x))
          seedDescendants(x, descendants);

        d.add(x);
        for (Integer y : descendants.get(x))
          d.add(y);
      }
      descendants.put(i, d);
    }
  }
}
