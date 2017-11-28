package carbyne.F2;

import carbyne.cluster.Cluster;
import carbyne.datastructures.BaseDag;
import carbyne.datastructures.StageDag;
import carbyne.datastructures.Task;
import carbyne.schedulers.InterJobScheduler;
import carbyne.schedulers.IntraJobScheduler;

import java.util.*;

public class ExecuteService {

  private Cluster cluster_;
  private InterJobScheduler interJobScheduler_;
  private IntraJobScheduler intraJobScheduler_;
  private Queue<BaseDag> runningJobs_;
  public Map<Task, Double> runningTasks_;

  public ExecuteService(Cluster cluster, InterJobScheduler interJobScheduler,
                        IntraJobScheduler intraJobScheduler,
                        Queue<BaseDag> runningJobs) {
    cluster_ = cluster;
    interJobScheduler_ = interJobScheduler;
    intraJobScheduler_ = intraJobScheduler;
    runningJobs_ = runningJobs;
  }

  public void receiveReadyEvents(boolean needInterJobScheduling, Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    if(needInterJobScheduling) {
      interJobScheduler_.schedule(cluster_);
    }
    ReadyEvent readyEvent = readyEventQueue.poll();
    while(readyEvent != null) {
      receiveReadyEvent(readyEvent);
      readyEvent = readyEventQueue.poll();
    }

    emitSpillEvents(spillEventQueue, 0);
  }

  private void receiveReadyEvent(ReadyEvent readyEvent) {
    //fetch data from the partition of this readyEvent
    int dagId = readyEvent.getDagId();
    Partition partition = readyEvent.getPartition();
    if(!partition.isLastPartReady()) { return; }

    List<Integer> machines = partition.getMachinesInvolved();
    double max = -1;
    int id = -1;
    for(Integer machineId : machines) {
      double cur = partition.getPartitionSizeOnMachine(machineId);
      if(max < cur) {
        max = cur;
        id = machineId;
      }
    }

    //group up data before executing tasks
    int taskId = -1;
    if(machines.size() > 1) {
      taskId = groupUpDataForTask(id, machines);
    }

    //now runnable tasks updated in Simulator::updateJobsStatus, need to modify
    //so that runnable tasks will be updated according to the ready events
    schedule(dagId, taskId);
  }

  private int groupUpDataForTask(int id, List<Integer> machines) {
    //copy data to a single node
    //generate task and return Id
    Task task = null;
    return task.taskId;
  }

  private void schedule(int dagId, int taskId) {
    BaseDag dag = null;
    for(BaseDag e : runningJobs_) {
      if(e.getDagId() == dagId) {
        dag = e;
        break;
      }
    }
    if(dag == null) {
      System.out.println("Error: Dag is not running any more when trying to schedule");
      return;
    }

    //add taskId to runnable tasks
    //Should also add duration and rsrcDemands? Because creating new tasks, no related info.
    dag.runnableTasks.add(taskId);

    intraJobScheduler_.schedule((StageDag) dag);

  }

  private void emitSpillEvents(Queue<SpillEvent> spillEventQueue, double currentTime) {
    for(Map.Entry<Task, Double> ele : runningTasks_.entrySet()) {
      if(ele.getValue() > currentTime) {
        //emit spill and remove this entry
      }
    }
  }
}
