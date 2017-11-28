package carbyne.schedpolicies;

import carbyne.cluster.Cluster;
import carbyne.datastructures.Resources;
import carbyne.datastructures.StageDag;

public abstract class SchedPolicy {

  protected Cluster cluster_;

  public SchedPolicy(Cluster cluster) {
    cluster_ = cluster;
  }

  public abstract void schedule(StageDag dag);

  public abstract double planSchedule(StageDag dag, Resources leftOverResources);
}
