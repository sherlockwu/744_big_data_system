package carbyne.simulator;

import carbyne.simulator.Main.Globals.JobsArrivalPolicy;
import carbyne.simulator.Main.Globals.RunMode;
import carbyne.simulator.Main.Globals.SchedulingPolicy;
import carbyne.simulator.Main.Globals.SharingPolicy;

import java.util.logging.Logger;

public class Main {

  private static Logger LOG = Logger.getLogger(Main.class.getName());
  public static class Globals {

    public enum RunMode {
      Robert, Mosharaf, CommandLine, GenerateTrace
    };

    public static RunMode runmode = RunMode.CommandLine;

    public static enum SchedulingPolicy {
      Random, BFS, CP, Tetris, Carbyne
    };

    public static SchedulingPolicy INTRA_JOB_POLICY = SchedulingPolicy.CP;

    public enum SharingPolicy {
      Fair, DRF, SJF, TETRIS_UNIVERSAL
    };

    public static SharingPolicy INTER_JOB_POLICY = SharingPolicy.Fair;

    public enum JobsArrivalPolicy {
      All, One, Distribution, Trace;
    }

    public static JobsArrivalPolicy JOBS_ARRIVAL_POLICY = JobsArrivalPolicy.All;

    public static int NUM_MACHINES, NUM_DIMENSIONS;
    public static double MACHINE_MAX_RESOURCE;
    public static int DagIdStart, DagIdEnd;

    public static boolean ADJUST_FUNGIBLE = false;

    public static double SIM_END_TIME = 20;
    public static double STEP_TIME = .1;

    public static int NUM_OPT = 0, NUM_PES = 0;

    public static int MAX_NUM_TASKS_DAG = 3000;

    public static boolean TETRIS_UNIVERSAL = false;
    /**
     * these variables control the sensitivity of the simulator to various factors
     * */
    // between 0.0 and 1.0; 0.0 it means jobs are not pessimistic at all
    public static double LEVEL_OF_OPTIMISM = 0.0;

    public static boolean COMPUTE_STATISTICS = false;
    public static double ERROR = 0.0;

    /**
     * these variables will be set by the static constructor based on runmode
     */
    public static String DataFolder;
    public static String FileOutput;
    public static String pathToInputDagFile = "inputs/dags-input0.json";
    public static String pathToConfig = "inputs/config.json";

    /*
    static {
      switch (runmode) {
      case Robert:
        String root = "/u/r/g/rgrandl/School/research/"
            + "bottleneck-agnostic-scheduling/workload";
        DataFolder = root + "/traces";
        FileInput = "50Jobs.txt";
        FileOutput = "dags-output.txt";
        pathToInputDagFile = DataFolder + "/" + FileInput;

        SIM_END_TIME = 500000;
        STEP_TIME = 1;

        NUM_MACHINES = 1;
        NUM_DIMENSIONS = 6;
        MACHINE_MAX_RESOURCE = 100;

        ADJUST_FUNGIBLE = false;
        JOBS_ARRIVAL_POLICY = JobsArrivalPolicy.All;

        DagIdStart = 0;
        DagIdEnd = 50;

        INTER_JOB_POLICY = SharingPolicy.Fair;
        INTRA_JOB_POLICY = SchedulingPolicy.CP;

        // sensitivity
        LEVEL_OF_OPTIMISM = 1.0;
        TETRIS_UNIVERSAL = false;
        COMPUTE_STATISTICS = true;
        ERROR = 0.0;
        break;
      case Mosharaf:
        String root1 = "/Users/mosharaf/Dropbox/Carbyne/";
        DataFolder = root1 + "workload/traces";
        LOG.info("Path: " + DataFolder);
        FileInput = "50Jobs.txt";
        FileOutput = "dags-output.txt";
        pathToInputDagFile = DataFolder + "/" + FileInput;

        SIM_END_TIME = 50000;
        STEP_TIME = 1;

        NUM_MACHINES = 1;
        NUM_DIMENSIONS = 6;
        MACHINE_MAX_RESOURCE = 100.0;

        ADJUST_FUNGIBLE = false;
        JOBS_ARRIVAL_POLICY = JobsArrivalPolicy.Trace;

        DagIdStart = 0;
        DagIdEnd = 1;

        INTER_JOB_POLICY = SharingPolicy.Fair;
        INTRA_JOB_POLICY = SchedulingPolicy.Carbyne;
        break;
      case CommandLine:
        break;
      case GenerateTrace:
        break;
      default:
        System.err.println("Unknown runmode");
      }
    } */
  }

  public static void main(String[] args) {

    String UsageStr = "Usage: java carbyne.simulator.Main pathToConfig pathToDags "
        + "time_step end_time adjust_fungible dag_id_end "
        + "inter_job_policy=[FAIR | DRF | SJF] "
        + "intra_job_policy=[CARBYNE | TETRIS | CP | BFS | RANDOM]"
        + " level_optimism([0.0 - 1.0])"
        + " compute_stats";

    // read parameters from command line, if specified
    if (Globals.runmode == RunMode.CommandLine) {
      int curArg = 0;

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.pathToConfig = args[curArg];   // pathToConfig
      curArg++;

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.pathToInputDagFile = args[curArg];   // pathToInput
      curArg++;

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.STEP_TIME = Double.parseDouble(args[curArg]); // time step size
      curArg++;

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.SIM_END_TIME = Double.parseDouble(args[curArg]); // time end
      curArg++;

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.ADJUST_FUNGIBLE = Boolean.parseBoolean(args[curArg]); // adjust_fungible
      curArg++;

      Globals.JOBS_ARRIVAL_POLICY = JobsArrivalPolicy.Trace;

      Globals.DagIdStart = 0;
      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.DagIdEnd = Integer.parseInt(args[curArg]);  // dag_id_end
      curArg++;

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      String UPPER_ARG = args[curArg].toUpperCase();  // inter_job_policy
      curArg++;
      if (UPPER_ARG.contains("FAIR")) {
        Globals.INTER_JOB_POLICY = SharingPolicy.Fair;
      } else if (UPPER_ARG.contains("DRF")) {
        Globals.INTER_JOB_POLICY = SharingPolicy.DRF;
      } else if (UPPER_ARG.contains("SJF")) {
        Globals.INTER_JOB_POLICY = SharingPolicy.SJF;
      } else {
        LOG.warning("UNKNOWN INTER_JOB_POLICY");
        LOG.info(UsageStr);
        System.exit(0);
      }

      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      UPPER_ARG = args[curArg].toUpperCase();  // intra_job_policy
      curArg++;
      if (UPPER_ARG.contains("CARBYNE")) {
        Globals.INTRA_JOB_POLICY = SchedulingPolicy.Carbyne;
      } else if (UPPER_ARG.contains("TETRIS")) {
        Globals.INTRA_JOB_POLICY = SchedulingPolicy.Tetris;
      } else if (UPPER_ARG.contains("CP")) {
        Globals.INTRA_JOB_POLICY = SchedulingPolicy.CP;
      } else if (UPPER_ARG.contains("BFS")) {
        Globals.INTRA_JOB_POLICY = SchedulingPolicy.BFS;
      } else if (UPPER_ARG.contains("RANDOM")) {
        Globals.INTRA_JOB_POLICY = SchedulingPolicy.Random;
      } else {
        LOG.warning("UNKNOWN INTRA_JOB_POLICY");
        LOG.info(UsageStr);
        System.exit(0);
      }

      // sensitivity
      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      double opt_arg = Double.parseDouble(args[curArg]); // level_optimism
      if (opt_arg < 0 || opt_arg > 1.0) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.LEVEL_OF_OPTIMISM = opt_arg;
      curArg++;
      if (args.length == curArg) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      
      boolean compute_stats = Boolean.parseBoolean(args[curArg]);  // compute_stats
      if (opt_arg < 0 || opt_arg > 1.0) {
        LOG.info(UsageStr);
        System.exit(0);
      }
      Globals.COMPUTE_STATISTICS = compute_stats;

    }
    /* 
    else if (Globals.runmode == RunMode.GenerateTrace) {
      String root = "/u/r/g/rgrandl/School/research/"
          + "bottleneck-agnostic-scheduling/workload";
      Globals.DataFolder = root + "/traces";
      Globals.FileInput = "queries_tpch.txt";
      Globals.FileOutput = "queries_tpch_no_distr.txt";
      Globals.pathToInputDagFile = Globals.DataFolder + "/" + Globals.FileInput;

      Globals.NUM_DIMENSIONS = 6;

      Globals.DagIdStart = 0;
      Globals.DagIdEnd = 40;

      Utils.generateTrace();
      System.exit(-1);
    } */

    // print ALL parameters for the record
    System.out.println("=====================");
    System.out.println("Simulation Parameters");
    System.out.println("=====================");
    System.out.println("pathToInputDagFile     = " + Globals.pathToInputDagFile);
    System.out.println("SIMULATION_END_TIME = " + Globals.SIM_END_TIME);
    System.out.println("STEP_TIME           = " + Globals.STEP_TIME);
    System.out.println("NUM_MACHINES        = " + Globals.NUM_MACHINES);
    System.out.println("NUM_DIMENSIONS      = " + Globals.NUM_DIMENSIONS);
    System.out.println("MACHINE_MAX_RESOURCE= " + Globals.MACHINE_MAX_RESOURCE);
    System.out.println("ADJUST_FUNGIBLE     = " + Globals.ADJUST_FUNGIBLE);
    System.out.println("JOBS_ARRIVAL_POLICY = " + Globals.JOBS_ARRIVAL_POLICY);
    System.out.println("DagIdStart          = " + Globals.DagIdStart);
    System.out.println("DagIdEnd            = " + Globals.DagIdEnd);
    System.out.println("INTER_JOB_POLICY    = " + Globals.INTER_JOB_POLICY);
    System.out.println("INTRA_JOB_POLICY    = " + Globals.INTRA_JOB_POLICY);
    System.out.println("LEVEL_OF_OPTIMISM   = " + Globals.LEVEL_OF_OPTIMISM);
    System.out.println("INTRODUCED RES.ERROR= " + Globals.ERROR);
    System.out.println("=====================\n");

    LOG.info("Start simulation ...");
    Simulator simulator = new Simulator();
    simulator.simulate();
    LOG.info("End simulation ...");
  }
}
