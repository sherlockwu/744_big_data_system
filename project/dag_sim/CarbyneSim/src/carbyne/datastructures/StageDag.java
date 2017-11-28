package carbyne.datastructures;

import carbyne.cluster.Cluster;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Simulator;
import carbyne.utils.Interval;

import java.util.*;
import java.util.logging.Logger;

public class StageDag extends BaseDag {

  private static Logger LOG = Logger.getLogger(StageDag.class.getName());
  public String dagName;
  private double quota_;

  public Map<String, Stage> stages;
  public Map<Integer, String> vertexToStage;  // <vertexId (taskID), stageName vertexId in>

  // keep track of ancestors and descendants of tasks per task
  public Map<Integer, Set<Integer>> ancestorsT, descendantsT,
      unorderedNeighborsT;
  public Map<String, Set<String>> ancestorsS, descendantsS,
      unorderedNeighborsS;

  public Set<String> chokePointsS;
  public Set<Integer> chokePointsT;

  // keep track of adjusted profiles for certain tasks;
  public Map<Integer, Task> adjustedTaskDemands = null;

  public StageDag(int id, double quota, int... arrival) {
    super(id, arrival);
    stages = new HashMap<String, Stage>();
    chokePointsS = new HashSet<String>();
    chokePointsT = null;
    quota_ = quota;
  }

  public StageDag(String dagName, int id, double quota, int... arrival) {
    super(id, arrival);
    stages = new HashMap<String, Stage>();
    chokePointsS = new HashSet<String>();
    chokePointsT = null;
    this.dagName = dagName;
    quota_ = quota;
  }

  public static StageDag clone(StageDag dag) {
    StageDag clonedDag = new StageDag(dag.dagId, dag.getQuota());
    clonedDag.dagName = dag.dagName;

    clonedDag.rsrcQuota = Resources.clone(dag.rsrcQuota);
    clonedDag.jobExpDur = dag.jobExpDur;

    if (dag.adjustedTaskDemands != null)
      clonedDag.adjustedTaskDemands = new HashMap<Integer, Task>(
          dag.adjustedTaskDemands);

    clonedDag.runnableTasks = new LinkedHashSet<Integer>(dag.runnableTasks);
    clonedDag.runningTasks = new LinkedHashSet<Integer>(dag.runningTasks);
    clonedDag.finishedTasks = new LinkedHashSet<Integer>(dag.finishedTasks);
    clonedDag.chokePointsS.addAll(dag.chokePointsS);

    clonedDag.CPlength = new HashMap<Integer, Double>(dag.CPlength);
    clonedDag.BFSOrder = new HashMap<Integer, Double>(dag.BFSOrder);

    for (Map.Entry<String, Stage> entry : dag.stages.entrySet()) {
      String stageName = entry.getKey();
      Stage stage = entry.getValue();
      clonedDag.stages.put(stageName, Stage.clone(stage));
    }

    clonedDag.vertexToStage = new HashMap<Integer, String>(dag.vertexToStage);
    clonedDag.ancestorsS = new HashMap<String, Set<String>>(dag.ancestorsS);
    clonedDag.descendantsS = new HashMap<String, Set<String>>(dag.descendantsS);
    clonedDag.unorderedNeighborsS = new HashMap<String, Set<String>>(
        dag.unorderedNeighborsS);

    clonedDag.ancestorsT = new HashMap<Integer, Set<Integer>>(dag.ancestorsT);
    clonedDag.descendantsT = new HashMap<Integer, Set<Integer>>(
        dag.descendantsT);

    return clonedDag;
  }

  // scale large DAGs to be handled by the simulator
  public void scaleDag() {

    int numTasksDag = allTasks().size();
    if (numTasksDag <= Globals.MAX_NUM_TASKS_DAG) {
      return;
    }

    double scaleFactor = 1.0;
    while (true) {
      scaleFactor *= 1.2;
      if ((int) Math.ceil((double) numTasksDag / scaleFactor) <= Globals.MAX_NUM_TASKS_DAG)
        break;
    }

    Map<Integer, String> vStartIdToStage = new TreeMap<Integer, String>();
    for (Stage stage : stages.values()) {
      vStartIdToStage.put(stage.vids.begin, stage.name);
    }

    Map<String, Integer> numTasksBefore = new HashMap<String, Integer>();
    int vertexIdxStart = 0, vertexIdxEnd = 0;
    for (int vIdStart : vStartIdToStage.keySet()) {
      String stageName = vStartIdToStage.get(vIdStart);

      int numVertices = stages.get(stageName).vids.Length();
      numTasksBefore.put(stageName, numVertices);
      numVertices = (int) Math.max(
          Math.ceil((double) numVertices / scaleFactor), 1);
      vertexIdxEnd += numVertices;

      stages.get(stageName).vids.begin = vertexIdxStart;
      stages.get(stageName).vids.end = vertexIdxEnd - 1;

      vertexIdxStart = vertexIdxEnd;
    }

    // reinitialize the mapping from vertices to stages
    vertexToStage.clear();
    for (Stage stage : stages.values()) {
      for (int i = stage.vids.begin; i <= stage.vids.end; i++) {
        vertexToStage.put(i, stage.name);
      }
    }

    // update vids for dependencies between stages
    List<Dependency> edges = new ArrayList<Dependency>();
    for (String stageSrc : stages.keySet()) {
      for (String stageDst : stages.get(stageSrc).children.keySet()) {
        edges.add(new Dependency(stageSrc, stageDst,
            stages.get(stageSrc).children.get(stageDst).type));
      }
    }

    // update new edge structure
    for (String stage : stages.keySet()) {
      stages.get(stage).children.clear();
      stages.get(stage).parents.clear();
    }

    for (Dependency dependency : edges) {
      this.populateParentsAndChildrenStructure(dependency.parent,
          dependency.child, dependency.type);
    }
  }

  public double getQuota() {return quota_;}

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
      System.out.print("Stage: " + stage.id + " "+stage.name+ " [");
      System.out.print(stage.vDuration + " ");
      for (int i = 0; i < Globals.NUM_DIMENSIONS; i++)
        System.out.print(stage.vDemands.resource(i) + " ");
      System.out.print("]\n");

      System.out.print("  Tasks:");
      for (int i = stage.vids.begin; i <= stage.vids.end; i++)
        System.out.print(i + " ");
      System.out.println();

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
    Dependency d = new Dependency(stage_src, stage_dst, comm_pattern,
        stages.get(stage_src).vids, stages.get(stage_dst).vids);

    stages.get(stage_src).children.put(stage_dst, d);
    stages.get(stage_dst).parents.put(stage_src, d);
  }

  // end read dags from file //

  // DAG traversals //
  @Override
  public void setCriticalPaths() {
    if (CPlength == null) {
      CPlength = new HashMap<Integer, Double>();
    }
    for (int vertexId : vertexToStage.keySet()) {
      longestCriticalPath(vertexId);
    }
  }

  @Override
  public double getMaxCP() {
    return Collections.max(CPlength.values());
  }

  @Override
  public double longestCriticalPath(int taskId) {
    if (CPlength != null && CPlength.containsKey(taskId)) {
      return CPlength.get(taskId);
    }

    if (CPlength == null) {
      CPlength = new HashMap<Integer, Double>();
    }

    double maxChildCP = Double.MIN_VALUE;
    String stageName = this.vertexToStage.get(taskId);

    List<Interval> children = this.getChildren(taskId);
    // System.out.println("Children: "+children);
    if (children.size() == 0) {
      maxChildCP = 0;
    } else {
      for (Interval i : children) {
        for (int child = i.begin; child <= i.end; child++) {
          double childCP = longestCriticalPath(child);
          if (maxChildCP < childCP) {
            maxChildCP = childCP;
          }
        }
      }
    }

    double cp = maxChildCP + stages.get(stageName).duration(taskId);
    if (!CPlength.containsKey(taskId)) {
      CPlength.put(taskId, cp);
    }

    return CPlength.get(taskId);
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

        int sb = stages.get(s).vids.begin;
        int se = stages.get(s).vids.end;
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

  // end DAG traversals //

  @Override
  public Resources rsrcDemands(int taskId) {
    if (adjustedTaskDemands != null && adjustedTaskDemands.get(taskId) != null) {
      return adjustedTaskDemands.get(taskId).resDemands;
    }
    return stages.get(vertexToStage.get(taskId)).rsrcDemands(taskId);
  }

  @Override
  public double duration(int taskId) {
    if (adjustedTaskDemands != null && adjustedTaskDemands.get(taskId) != null) {
      return adjustedTaskDemands.get(taskId).taskDuration;
    }
    return stages.get(vertexToStage.get(taskId)).duration(taskId);
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
        bef += stage.vids.Length() * stage.vDuration * stage.vDemands.resources[i];
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

  // return true or false -> based on if this job has finished or not
  public boolean finishTasks(Cluster cluster, 
      List<Integer> completedTasks, boolean reverse) {

    if (completedTasks.isEmpty()) return false;

    // move finishedTasks from runningTasks into finishedTasks
    assert (runningTasks.containsAll(completedTasks));
    runningTasks.removeAll(completedTasks);
    finishedTasks.addAll(completedTasks);
    for (int i = 0; i < completedTasks.size(); i++) {
      Integer taskToRemove = completedTasks.get(i);
    }
/*    for (int fTask : completedTasks) {
      assert (runningTasks.contains(fTask));
      runningTasks.remove((Integer) fTask);
      finishedTasks.add(fTask);
    }
*/
    // all tasks have finished -> job has completed
    if (finishedTasks.size() == allTasks().size()) {
      jobEndTime = Simulator.CURRENT_TIME;
      return true;
    }

    List<Integer> tasksRemToBeSched = new ArrayList<Integer>(allTasks());
    tasksRemToBeSched.removeAll(runnableTasks);
    tasksRemToBeSched.removeAll(runningTasks);
    tasksRemToBeSched.removeAll(finishedTasks);
    for (int candTask : tasksRemToBeSched) {
      boolean candTaskReadyToSched = true;
      List<Interval> depCandTasks = (!reverse) ? getParents(candTask)
          : getChildren(candTask);
      for (Interval ival : depCandTasks) {
        if (!candTaskReadyToSched) break;
        List<Integer> ivalList = ival.toList();
        for (Integer taskId : ivalList) {
          if (!cluster.containsIntermediateResult(taskId)) {
            LOG.fine("Task " + candTask + " is not ready. Its parent " + taskId + " is still running.");
            candTaskReadyToSched = false;
            break;
          }
        }
      }
      if (candTaskReadyToSched) {
        LOG.fine("Task " + candTask + " is now runnable.");
        runnableTasks.add(candTask);
      }

    }

/*
    // enable new runnableTasks if any
    // RG: expensive operation here - TODO (optimize)
    // for every task check if his parents are in finishedTasks
    for (int candTask : allTasks()) {
      if (runnableTasks.contains(candTask) || runningTasks.contains(candTask)
          || finishedTasks.contains(candTask)) {
        continue;
      }

      boolean candTaskReadyToSched = true;
      List<Interval> depCandTask = (!reverse) ? getParents(candTask)
          : getChildren(candTask);

      for (Interval ival : depCandTask) {
        for (int i = ival.begin; i <= ival.end; i++) {
          if (!finishedTasks.contains(i)) {
            candTaskReadyToSched = false;
            break;
          }
        }
      }

      if (candTaskReadyToSched) {
        runnableTasks.add(candTask);
      }
    }
*/
    return false;
  }

  // should decrease only the resources allocated in the current time quanta
  public Resources currResShareAvailable() {
    Resources totalShareAllocated = Resources.clone(this.rsrcQuota);

    for (int task : launchedTasksNow) {
      Resources rsrcDemandsTask = rsrcDemands(task);
      totalShareAllocated.subtract(rsrcDemandsTask);
    }
    totalShareAllocated.normalize();
    return totalShareAllocated;
  }

  public void seedUnorderedNeighbors() {

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
  }

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
