package carbyne.utils;

import java.io.FileReader;
import java.util.Queue;
import java.util.LinkedList;
import java.util.HashMap;

import carbyne.datastructures.BaseDag;
import carbyne.datastructures.StageDag;
import carbyne.datastructures.Stage;
import carbyne.datastructures.Dependency;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class DagParser {
  private JSONParser parser_;
  private int taskIDEnd_;

  public DagParser() {
    parser_ = new JSONParser();
    taskIDEnd_ = 0;
  }
  
  public Queue<BaseDag> parseDAGSpecFile(String filePath) {
    Queue<BaseDag> dags = new LinkedList<BaseDag>();
    try {
      FileReader fr = new FileReader(filePath);
      JSONArray jDags = (JSONArray)parser_.parse(fr);

      for (Object jDag: jDags) {
        dags.add(parseDAG((JSONObject)jDag));
      }
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }
    return dags;
  }

  public StageDag parseDAG(JSONObject jDag) {
    StageDag dag = new StageDag(jDag.get("name").toString(), 
        Integer.parseInt(jDag.get("dagID").toString()), 
        Integer.parseInt(jDag.get("arrival_time").toString()));

    JSONArray jStages = (JSONArray)jDag.get("stages");
    for (int i = 0; i < jStages.size(); i++) {
      JSONObject jStage = (JSONObject)jStages.get(i);
      dag.stages.put(jStage.get("name").toString(), parseStage(jStage, i));
    }
    JSONArray jDeps = (JSONArray)jDag.get("dependencies");
    for (Object jDep: jDeps) {
      parseDependency((JSONObject)jDep, dag);
    }

    dag.vertexToStage = new HashMap<Integer, String>();
    for (Stage stage : dag.stages.values())
      for (int i = stage.vids.begin; i <= stage.vids.end; i++)
        dag.vertexToStage.put(i, stage.name);

    dag.scaleDag();
    dag.setCriticalPaths();
    dag.setBFSOrder();
    for (int taskId : dag.allTasks()) {
      if (dag.getParents(taskId).isEmpty()) {
        dag.runnableTasks.add(taskId);
      }
    }
    return dag;
  }

  public Stage parseStage(JSONObject jStage, int i) {
    int taskIDStart = taskIDEnd_;
    taskIDEnd_ += Integer.parseInt(jStage.get("num_tasks").toString());
    return new Stage(jStage.get("name").toString(), i,
          new Interval(taskIDStart, taskIDEnd_ - 1),
          Double.parseDouble(jStage.get("duration").toString()),
          ((JSONArray)jStage.get("resources")).stream().mapToDouble(x -> Double.valueOf(x.toString()) ).toArray());
  }

  public void parseDependency(JSONObject jDep, StageDag dag) {
    dag.populateParentsAndChildrenStructure(jDep.get("src").toString(),
        jDep.get("dst").toString(), jDep.get("pattern").toString());
  }
}
