package carbyne.simulator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import carbyne.cluster.Cluster;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Resources;
import carbyne.datastructures.Stage;
import carbyne.datastructures.StageDag;
import carbyne.resources.LeftOverResAllocator;
import carbyne.schedulers.InterJobScheduler;
import carbyne.schedulers.IntraJobScheduler;
import carbyne.simulator.Main.Globals;
import carbyne.simulator.Main.Globals.JobsArrivalPolicy;
import carbyne.utils.Pair;
import carbyne.utils.Randomness;
import carbyne.utils.Triple;
import carbyne.utils.Utils;

// implement the timeline server
public class Simulator {

  public static double CURRENT_TIME = 0;

  public static Queue<BaseDag> runnableJobs;
  public static Queue<BaseDag> runningJobs;
  public static Queue<BaseDag> completedJobs;

  public static Cluster cluster;

  public static Randomness r;
  double nextTimeToLaunchJob = 0;

  int totalReplayedJobs;
  int lastCompletedJobs;

  public static InterJobScheduler interJobSched;
  public static IntraJobScheduler intraJobSched;

  public static LeftOverResAllocator leftOverResAllocator;

  // dag_id -> list of tasks
  public static Map<Integer, Set<Integer>> tasksToStartNow;

  public Simulator() {
    runnableJobs = StageDag.readDags(Globals.PathToInputFile,
        Globals.DagIdStart, Globals.DagIdEnd - Globals.DagIdStart + 1);
     // System.out.println("Print DAGs");
     // for (BaseDag dag : runnableJobs) {
     // ((StageDag) dag).viewDag();
     //}

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
    cluster = new Cluster(true, new Resources(Globals.MACHINE_MAX_RESOURCE));

    interJobSched = new InterJobScheduler();
    intraJobSched = new IntraJobScheduler();

    leftOverResAllocator = new LeftOverResAllocator();

    tasksToStartNow = new TreeMap<Integer, Set<Integer>>();

    r = new Randomness();
  }

  public void simulate() {

    for (Simulator.CURRENT_TIME = 0; Simulator.CURRENT_TIME < Globals.SIM_END_TIME; Simulator.CURRENT_TIME += Globals.STEP_TIME) {
      System.out.println("\n==== STEP_TIME:" + Simulator.CURRENT_TIME
          + " ====\n");

      Simulator.CURRENT_TIME = Utils.round(Simulator.CURRENT_TIME, 2);
      tasksToStartNow.clear();

      // terminate any task if it can finish and update cluster available
      // resources
      Map<Integer, List<Integer>> finishedTasks = cluster.finishTasks();

      // update jobs status with newly finished tasks
      boolean jobCompleted = updateJobsStatus(finishedTasks);

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

      if (!jobCompleted && !newJobArrivals && finishedTasks.isEmpty()) {
        System.out.println("\n==== END STEP_TIME:" + Simulator.CURRENT_TIME
            + " ====\n");
        continue;
      }

      if (Globals.TETRIS_UNIVERSAL) {
        interJobSched.resSharePolicy.packTasks();
      } else {
        System.out.println("[Simulator]: jobCompleted:" + jobCompleted
          + " newJobArrivals:" + newJobArrivals);
        if (jobCompleted || newJobArrivals)
          interJobSched.schedule();

        System.out.println("Running jobs size:" + runningJobs.size());

        // reallocate the share
        interJobSched.adjustShares();

        // do intra-job scheduling for every running job
        if (Globals.INTRA_JOB_POLICY != Globals.SchedulingPolicy.Carbyne) {

          for (BaseDag dag : runningJobs) {
            // System.out.println("[Simulator]: intra scheduleDag for:" +
            // dag.dagId);
            intraJobSched.schedule((StageDag) dag);
          }

          // if still available resources, go one job at a time and fill if
          // something. can be scheduled more
          System.out.println("[Simulator]: START work conserving; clusterAvail:"
              + Simulator.cluster.getClusterResAvail());

          // while things can happen, give total resources to a job at a time,
          // the order is dictated by the inter job scheduler:
          // Shortest JobFirst - for SJF
          // RR - for Fair and DRF
          List<Integer> orderedJobs = interJobSched
              .orderedListOfJobsBasedOnPolicy();
          for (int jobId : orderedJobs) {
            for (BaseDag dag : runningJobs) {
              if (dag.dagId == jobId) {
                Resources totalResShare = Resources.clone(dag.rsrcQuota);
                dag.rsrcQuota = Resources.clone(cluster.getClusterResAvail());
                intraJobSched.schedule((StageDag) dag);
                dag.rsrcQuota = totalResShare;
                break;
              }
            }
          }

          System.out.println("[Simulator]: END work conserving; clusterAvail:"
              + Simulator.cluster.getClusterResAvail());
        } else {
          // compute if any tasks should be scheduled based on reverse schedule
          for (BaseDag dag : runningJobs) {
            dag.timeToComplete = intraJobSched.planSchedule((StageDag) dag, null);
          }
          // System.out.println("Tasks which should start as of now:"
          // + Simulator.tasksToStartNow);

          // schedule the DAGs -> looking at the list of tasksToStartNow
          for (BaseDag dag : runningJobs) {
            intraJobSched.schedule((StageDag) dag);
          }

          // Step2: redistribute the leftOverResources and ensuring is work
          // conserving
          leftOverResAllocator.allocLeftOverRsrcs();
        } 
      }
      System.out.println("\n==== END STEP_TIME:" + Simulator.CURRENT_TIME
          + " ====\n");
    }
  }

  boolean stop() {
    return (runnableJobs.isEmpty() && runningJobs.isEmpty() && (completedJobs
        .size() == totalReplayedJobs));
  }

  boolean updateJobsStatus(Map<Integer, List<Integer>> finishedTasks) {
    boolean someDagFinished = false;
    if (!finishedTasks.isEmpty()) {
      Iterator<BaseDag> iter = runningJobs.iterator();
      while (iter.hasNext()) {
        BaseDag crdag = iter.next();
        if (finishedTasks.get(crdag.dagId) == null) {
          continue;
        }

        System.out.println("DAG:" + crdag.dagId + ": "
            + finishedTasks.get(crdag.dagId).size()
            + " tasks finished at time:" + Simulator.CURRENT_TIME);
        someDagFinished = ((StageDag) crdag).finishTasks(
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
  }

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
