package carbyne.F2;

import java.util.Map;
import java.util.HashMap;
import java.util.Queue;

public class DataService {
  private double[] keyShares_;
  private double[] quota_;  // quota per job
  private int numGlobalPart_;
  private Map<Integer, double[]> usage_;   // <DagId, list<usage>>
  private Map<Integer, Map<Integer, StageOutput>> stageOutput_;  // <DagId, <StageId, intermediate data>>

  public DataService(double[] keyShares, double[] quota, int numGlobalPart) {
    keyShares_ = keyShares;
    quota_ = quota;
    numGlobalPart_ = numGlobalPart;
    usage_ = new HashMap<>();
    stageOutput_ = new HashMap<>();
  }

  public void receiveSpillEvents(Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    SpillEvent event = spillEventQueue.poll();
    Map<Integer, Partition> readyParts = null;
    while (event != null) {
      readyParts = receiveSpillEvent(event);
      for (Map.Entry<Integer, Partition> part: readyParts.entrySet()) {
        readyEventQueue.add(new ReadyEvent(event.getDagId(), event.getStageId(), part.getKey(), part.getValue()));
      }
      event = spillEventQueue.poll();
    }
  }

  public Map<Integer, Partition> receiveSpillEvent(SpillEvent event) {
    Map<Integer, Partition> readyParts = new HashMap<>();
    int dagId = event.getDagId();
    int stageId = event.getStageId();
    if (!usage_.containsKey(dagId)) {
      usage_.put(dagId, new double[quota_.length]);
    }
    if (!stageOutput_.containsKey(dagId)) {
      stageOutput_.put(dagId, new HashMap<Integer, StageOutput>());
    }
    if (!stageOutput_.get(dagId).containsKey(stageId)) {
      stageOutput_.get(dagId).put(stageId, new StageOutput(usage_.get(dagId), quota_[dagId], numGlobalPart_));
    }
    Map<Integer, Double> newUsage = stageOutput_.get(dagId).get(stageId).materialize(event.getData());
    for (Map.Entry<Integer, Double> entry: newUsage.entrySet()) {
      usage_.get(dagId)[entry.getKey()] += entry.getValue();
    }

    // TODO: spread data
    
    // mark complete
    if (event.isLastSpill()) {
       stageOutput_.get(dagId).get(stageId).markComplete();
    }
    return stageOutput_.get(dagId).get(stageId).getReadyPartitions();
  }
}
