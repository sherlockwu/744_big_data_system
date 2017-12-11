package carbyne.utils;

import carbyne.datastructures.BaseDag;
import carbyne.datastructures.Stage;
import carbyne.datastructures.StageDag;
import carbyne.simulator.Main.Globals;

import java.io.*;
import java.util.*;

public class Utils {

  // generate a trace
  // read a bunch of dags
  // take a time distribution
  // generate a trace with N number of jobs
  // following the arrival times given
  // either trace or distribution
  // if no time distribution, just N number of jobs
  // otherwise, add the time as well
  public static void generateTrace() {

    int numJobs = 5000;
    Map<Integer, BaseDag> inputJobsMap = new HashMap<Integer, BaseDag>();
    DagParser dagParser = new DagParser();
    Queue<BaseDag> inputJobs = dagParser.parseDAGSpecFile(Globals.pathToInputDagFile);;
    for (BaseDag dag : inputJobs) {
      inputJobsMap.put(dag.dagId, dag);
    }
    int numInputDags = inputJobs.size();
    int nextDagId = 0;

    Randomness r = new Randomness();
    // read the time distribution
    List<Integer> timeline = timeDistribution();
    if (timeline != null) {
      for (Integer time : timeline) {
        // for every time step
        int nextDagIdx = r.pickRandomInt(numInputDags);
        //take a random DAG and write it to file with next dag ID
        BaseDag nextDag = inputJobsMap.get(nextDagIdx);
        nextDag.dagId = nextDagId;
        nextDag.timeArrival = time;
        nextDagId++;
        Utils.writeDagToFile((StageDag)nextDag, true);
      }
    }
    else {
      // generate random numJobs jobs in a trace
      while (numJobs > 0) {
        // for every time step
        int nextDagIdx = r.pickRandomInt(numInputDags);
        //take a random DAG and write it to file with next dag ID
        BaseDag nextDag = inputJobsMap.get(nextDagIdx);
        nextDag.dagId = nextDagId;
        nextDagId++;
        Utils.writeDagToFile((StageDag)nextDag, false);
        numJobs--;
      }
    }
  }

  public static List<Integer> timeDistribution() {

    String fileTimeDistribution = Globals.DataFolder+"/FBdistribution.txt";
    File file = new File(fileTimeDistribution);
    if (!file.exists()) return null;

    List<Integer> timeline = new ArrayList<Integer>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        timeline.add(Integer.parseInt(line));
      }
      br.close();
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }

    return timeline;
  }

  private static List<Integer> trimBox(List<String> strList) {
    List<Integer> res = new ArrayList<>();
//    for (int i = 0; i < l; i++) {
//      List<Integer> currentLineInteger = new ArrayList<>();
//      String cur = strList.get(i);
//      StringBuilder stringBuilder = new StringBuilder();
//
//      int stringLength = cur.length();
//      for (int j = 0; j < stringLength; j++) {
//        char c = cur.charAt(j);
//        if (c == ' ' || c == '\n')
//          continue;
//        stringBuilder.append(c);
//      }
//      String finalString = stringBuilder.toString();
//      currentLineInteger.add(Integer.parseInt(finalString));
//    }
    for(String str : strList) {
      res.add(Integer.parseInt(str.trim()));
    }
    return res;
  }

  public static List<List<List<Integer>>> parseDeploy() {
    String fileName = Globals.DataFolder+"/deploy.txt";
    File file = new File(fileName);
    List<List<Integer>> deploy = new ArrayList<>();
    List<List<List<Integer>>> res = new ArrayList<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        List<String> strList = Arrays.asList(line.split(","));
        List<Integer> currentStage = trimBox(strList);

        deploy.add(currentStage);
      }
      br.close();
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }
    res.add(deploy);
    return res;
  }

  public static void writeDagToFile(StageDag dag, boolean considerTimeDistr) {
    File file = new File(Globals.DataFolder+"/"+Globals.FileOutput);
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
      bw.write("# "+dag.dagName+"\n");
      if (considerTimeDistr) {
        bw.write(dag.stages.size()+" "+dag.dagId+" "+dag.timeArrival+"\n");
      }
      else {
        bw.write(dag.stages.size()+" "+dag.dagId+" "+"\n");
      }
      for (Stage stage : dag.stages.values()) {
        bw.write(stage.name+" "+stage.vDuration+" ");
        for (int i = 0; i < Globals.NUM_DIMENSIONS; i++) {
          bw.write(stage.vDemands.resource(i)+" ");
        }
        bw.write(stage.getNumTasks()+"\n");
      }
      int numEdges = 0;
      for (Stage stage : dag.stages.values()) {
        numEdges += stage.children.size();
      }
      bw.write(numEdges+"\n");
      for (Stage stage : dag.stages.values()) {
        for (String child : stage.children.keySet()) {
          bw.write(stage.name+" "+child+" "+"ata"+"\n");
        }
      }
      bw.close();
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }
  }

  public static double round(double value, int places) {
    double roundedVal = value;
    roundedVal = roundedVal * 100;
    roundedVal = Math.round(roundedVal);
    roundedVal /= 100;
    return roundedVal;
  }
}
