package carbyne.simulator;

import carbyne.F2.DataService;
import carbyne.F2.ExecuteService;
import carbyne.F2.ReadyEvent;
import carbyne.F2.SpillEvent;
import carbyne.cluster.Cluster;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Stage;
import carbyne.datastructures.StageDag;
import carbyne.resources.LeftOverResAllocator;
import carbyne.schedulers.InterJobScheduler;
import carbyne.schedulers.IntraJobScheduler;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Main.Globals.JobsArrivalPolicy;
import carbyne.utils.Configuration;
import carbyne.utils.DagParser;
import carbyne.utils.Randomness;
import carbyne.utils.Utils;

import java.util.*;
import java.util.logging.Logger;


// implement the timeline server
public class Simulator {
  private static Logger LOG = Logger.getLogger(Simulator.class.getName());

  public static double CURRENT_TIME = 0;

  public static Queue<BaseDag> runnableJobs;
  public static Queue<BaseDag> runningJobs;
  public static Queue<BaseDag> completedJobs;

  private Cluster cluster_;

  public static Randomness r;
  double nextTimeToLaunchJob = 0;

  int totalReplayedJobs;
  int lastCompletedJobs;

  public static InterJobScheduler interJobSched;
  public static IntraJobScheduler intraJobSched;

  public static LeftOverResAllocator leftOverResAllocator;

  // dag_id -> list of tasks
  public static Map<Integer, Set<Integer>> tasksToStartNow;

  private Queue<SpillEvent> spillEventQueue_;
  private Queue<ReadyEvent> readyEventQueue_;

  private DataService ds;
  private ExecuteService es;

  public Cluster getCluster() { return cluster_; }

  public Simulator() {
    DagParser dagParser = new DagParser();
    runnableJobs = dagParser.parseDAGSpecFile(Globals.pathToInputDagFile);
    Configuration config = new Configuration();
    config.parseConfigFile(Globals.pathToConfig);
    // double[] keySizes = dagParser.parseInputData(Globals.pathToInputDataFile);
    List<Double> quota = new ArrayList<Double>();
    for (BaseDag dag: runnableJobs) {
      quota.add(((StageDag)dag).getQuota());
    }
    spillEventQueue_ = new LinkedList<SpillEvent>();
    readyEventQueue_ = new LinkedList<ReadyEvent>();

    /* System.out.println("Key sizes:");
    for (int i = 0; i < keySizes.length; i++) {
      System.out.print(i + ":" + keySizes[i] + ", ");
    } */

    System.out.println("Print DAGs");
      for (BaseDag dag : runnableJobs) {
      ((StageDag) dag).viewDag();
    }

    if (Globals.COMPUTE_STATISTICS) {

      double[] area_makespan = new double[Globals.NUM_DIMENSIONS];
      System.out.println("#dag_id maxCP area");
      double total_area = 0.0;
      for (BaseDag dag : runnableJobs) {
        StageDag ddag = (StageDag)dag;
        double[] bottlenecks = new double[Globals.NUM_DIMENSIONS];
        for (Stage stage : ddag.stages.values()) {
          bottlenecks[stage.vDemands.resBottleneck()] += 1;
        }
        System.out.print(ddag.dagName+" "+ddag.stages.values().size());
        for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
          System.out.print(" "+bottlenecks[i]/ddag.stages.values().size());
        }
        System.out.print("\n");
      }
      // System.exit(-1);
      for (BaseDag dag : runnableJobs) {
        for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
          area_makespan[i] += dag.area().get(i);
        }
        double areaJob = (double) Collections.max(dag.area().values()) / Globals.MACHINE_MAX_RESOURCE;
        double maxCPJob= dag.getMaxCP();
        System.out.println(dag.dagId+" "+maxCPJob+" "+areaJob);
        total_area += areaJob;
      }
      double max_area_makespan = Double.MIN_VALUE;
      for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
        max_area_makespan = Math.max(max_area_makespan, area_makespan[i] / Globals.MACHINE_MAX_RESOURCE);
      }
      System.out.println("makespan_lb: "+ total_area + " " + max_area_makespan);
      System.exit(-1);
    }

    totalReplayedJobs = runnableJobs.size();
    runningJobs = new LinkedList<BaseDag>();
    completedJobs = new LinkedList<BaseDag>();

    // every machine has capacity == 1
    // cluster_ = new Cluster(true, new Resources(Globals.MACHINE_MAX_RESOURCE));
    cluster_ = new Cluster(true);
    config.populateCluster(cluster_);
    interJobSched = new InterJobScheduler(cluster_);
    intraJobSched = new IntraJobScheduler(cluster_);

    ds = new DataService(quota.stream().mapToDouble(v -> v).toArray(), config.getNumGlobalPart(), cluster_.getMachines().size());
    //TODO: export topology to es
    es = new ExecuteService(cluster_, interJobSched, intraJobSched, runningJobs, config.getMaxPartitionsPerTask());

    leftOverResAllocator = new LeftOverResAllocator();

    tasksToStartNow = new TreeMap<Integer, Set<Integer>>();

    r = new Randomness();
  }

  public void simulate() {

    for (Simulator.CURRENT_TIME = 0; Simulator.CURRENT_TIME < Globals.SIM_END_TIME; Simulator.CURRENT_TIME += Globals.STEP_TIME) {
      LOG.info("\n==== STEP_TIME:" + Simulator.CURRENT_TIME
          + " ====\n");

      Simulator.CURRENT_TIME = Utils.round(Simulator.CURRENT_TIME, 2);
      tasksToStartNow.clear();

      // terminate any task if it can finish and update cluster available
      // resources
      // Map<Integer, List<Integer>> finishedTasks = cluster_.finishTasks();

      // update jobs status with newly finished tasks
      // boolean jobCompleted = updateJobsStatus(finishedTasks);
      // TODO: stop condition
      boolean jobCompleted = es.finishTasks(spillEventQueue_);

      // stop condition
      if (stop()) {
        System.out.println("==== Final Report: Completed Jobs ====");
        TreeMap<Integer, Double> results = new TreeMap<Integer, Double>();
        double makespan = Double.MIN_VALUE;
        double average = 0.0;
        for (BaseDag dag : completedJobs) {
          double jobDur = (dag.jobEndTime - dag.jobStartTime);
        //  System.out.println("Dag:" + dag.dagId + " compl. time:"
        //      + (dag.jobEndTime - dag.jobStartTime));
          double dagDuration = (dag.jobEndTime - dag.jobStartTime);
          makespan = Math.max(makespan, dagDuration);
          average += dagDuration;
          results.put(dag.dagId, (dag.jobEndTime - dag.jobStartTime));
        }
        average /= completedJobs.size();
        System.out.println("---------------------");
        System.out.println("Avg. job compl. time:" + average);
        System.out.println("Makespan:" + makespan);
        for (Integer dagId : results.keySet()) {
          System.out.println(dagId+" "+results.get(dagId));
        }
        System.out.println("NUM_OPT:"+Globals.NUM_OPT+" NUM_PES:"+Globals.NUM_PES);
        break;
      }

      // handle jobs completion and arrivals
      boolean newJobArrivals = handleNewJobArrival();
      boolean needInterJobScheduling = newJobArrivals || jobCompleted;

      if (!jobCompleted && !newJobArrivals && spillEventQueue_.isEmpty()) {
        LOG.info("\n==== END STEP_TIME:" + Simulator.CURRENT_TIME
            + " ====\n");
        continue;
      }

      // TODO: put these scheduling process into ES
      LOG.info("[Simulator]: jobCompleted:" + jobCompleted
        + " newJobArrivals:" + newJobArrivals);

      LOG.info("Spillevent queue size: " + spillEventQueue_.size());
      ds.receiveSpillEvents(spillEventQueue_, readyEventQueue_);
      LOG.info("Readyevent queue size: " + readyEventQueue_.size());
      es.receiveReadyEvents(needInterJobScheduling, readyEventQueue_);

      LOG.info("\n==== END STEP_TIME:" + Simulator.CURRENT_TIME
          + " ====\n");
    }
  }

  boolean stop() {
    return (runnableJobs.isEmpty() && runningJobs.isEmpty() && (completedJobs
        .size() == totalReplayedJobs));
  }

  /* boolean updateJobsStatus(Map<Integer, List<Integer>> finishedTasks) {
    boolean someDagFinished = false;
    if (!finishedTasks.isEmpty()) {
      Iterator<BaseDag> iter = runningJobs.iterator();
      while (iter.hasNext()) {
        BaseDag crdag = iter.next();
        if (finishedTasks.get(crdag.dagId) == null) {
          continue;
        }

        System.out.print("DAG:" + crdag.dagId + ": "
            + finishedTasks.get(crdag.dagId).size()
            + " tasks finished at time:" + Simulator.CURRENT_TIME + ": [");
        finishedTasks.get(crdag.dagId).stream().forEach(taskId -> System.out.print(taskId + ","));
        System.out.println("]");
        someDagFinished = ((StageDag) crdag).finishTasks(cluster_,
            finishedTasks.get(crdag.dagId), false);

        if (someDagFinished) {
          System.out.println("DAG:" + crdag.dagId + " finished at time:"
              + Simulator.CURRENT_TIME);
          nextTimeToLaunchJob = Simulator.CURRENT_TIME;
          completedJobs.add(crdag);

          iter.remove();
        }
      }
    }
    return someDagFinished;
  } */

  boolean handleNewJobArrival() {
    // flag which specifies if jobs have inter-arrival times or starts at t=0
    System.out.println("handleNewJobArrival; currentTime:"
        + Simulator.CURRENT_TIME + " nextTime:" + nextTimeToLaunchJob);
    if (runnableJobs.isEmpty()
        || ((nextTimeToLaunchJob != Simulator.CURRENT_TIME) && (Globals.JOBS_ARRIVAL_POLICY != JobsArrivalPolicy.Trace))) {
      return false;
    }

    // start all jobs at time = 0
    if (Globals.JOBS_ARRIVAL_POLICY == JobsArrivalPolicy.All) {
      while (!runnableJobs.isEmpty()) {
        BaseDag newJob = runnableJobs.poll();
        newJob.jobStartTime = Simulator.CURRENT_TIME;
        runningJobs.add(newJob);
        System.out.println("Started job:" + newJob.dagId + " at time:"
            + Simulator.CURRENT_TIME);
      }
    }
    else if (Globals.JOBS_ARRIVAL_POLICY == JobsArrivalPolicy.One) {
      // if no job is running -> poll and add
      if (Simulator.runningJobs.isEmpty()) {
        BaseDag newJob = runnableJobs.poll();
        newJob.jobStartTime = Simulator.CURRENT_TIME;
        runningJobs.add(newJob);
        runnableJobs.remove(newJob);
        System.out.println("Started job:" + newJob.dagId + " at time:"
            + Simulator.CURRENT_TIME);
      }
    }
    // start one job at a time
    // compute the next time to launch a job using a distribution
    else if (Globals.JOBS_ARRIVAL_POLICY == JobsArrivalPolicy.Distribution) {

      do {
        BaseDag newJob = runnableJobs.poll();
        assert newJob != null;
        newJob.jobStartTime = Simulator.CURRENT_TIME;

        runningJobs.add(newJob);
        nextTimeToLaunchJob = Utils.round(
            Math.max(Math.ceil(r.GetNormalSample(20, 5)), 2), 0);
        nextTimeToLaunchJob += Simulator.CURRENT_TIME;
        System.out.println("Started job:" + newJob.dagId + " at time:"
            + Simulator.CURRENT_TIME + " next job arrives at time:"
            + nextTimeToLaunchJob);
      } while (!runnableJobs.isEmpty()
          && (nextTimeToLaunchJob == Simulator.CURRENT_TIME));
    } else if (Globals.JOBS_ARRIVAL_POLICY == JobsArrivalPolicy.Trace) {
      Set<BaseDag> newlyStartedJobs = new HashSet<BaseDag>();
      for (BaseDag dag : runnableJobs) {
        if (dag.timeArrival == Simulator.CURRENT_TIME) {
          dag.jobStartTime = Simulator.CURRENT_TIME;
          newlyStartedJobs.add(dag);
          System.out.println("Started job:" + dag.dagId + " at time:"
              + Simulator.CURRENT_TIME);
        }
      }
      // clear the datastructures
      runnableJobs.removeAll(newlyStartedJobs);
      runningJobs.addAll(newlyStartedJobs);
    }

    return true;
  }

  boolean handleNewJobCompleted() {
    int currCompletedJobs = completedJobs.size();
    if (lastCompletedJobs < currCompletedJobs) {
      lastCompletedJobs = currCompletedJobs;
      return true;
    }
    return false;
  }

  public static StageDag getDag(int dagId) {
    for (BaseDag dag : Simulator.runningJobs) {
      if (dag.dagId == dagId) {
        return (StageDag) dag;
      }
    }
    return null;
  }
}
